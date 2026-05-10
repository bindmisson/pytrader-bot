# ============================================================
# OPENALGO EXECUTION BOT — SAAS + FAILOVER + VALIDATION
# ============================================================

from fastapi import FastAPI, Request, HTTPException
import uvicorn
import os
import json
import logging
import asyncio
import math
import time
from datetime import datetime, timedelta, time as dt_time
from decimal import Decimal
from collections import defaultdict
from contextlib import asynccontextmanager

import boto3
from boto3.dynamodb.conditions import Key
from dotenv import load_dotenv
from openalgo import api
import pytz


print("🚀 Execution Bot Running — Production SaaS Mode")

# ============================================================
# ENV
# ============================================================
load_dotenv()

CLIENT_ID      = os.getenv("CLIENT_ID")
AWS_REGION     = os.getenv("AWS_REGION","ap-south-1")
DEFAULT_TABLE  = os.getenv("DEFAULT_TABLE","DefaultAlgoConfig")
OVERRIDE_TABLE = os.getenv("OVERRIDE_TABLE","ClientAlgoConfig")

if not CLIENT_ID:
    raise ValueError("CLIENT_ID missing")

# ============================================================
# INDEX / EXCHANGE / LOT MAPS (MULTI-INDEX)
# ============================================================
INDEX_EXCHANGE_MAP = {
    "NIFTY": "NSE_INDEX",
    "BANKNIFTY": "NSE_INDEX",
    "FINNIFTY": "NSE_INDEX",
    "MIDCPNIFTY": "NSE_INDEX",
    "NIFTYNXT50": "NSE_INDEX",
    "SENSEX": "BSE_INDEX",
    "BANKEX": "BSE_INDEX",
}

ORDER_EXCHANGE_MAP = {
    "NIFTY": "NFO",
    "BANKNIFTY": "NFO",
    "FINNIFTY": "NFO",
    "MIDCPNIFTY": "NFO",
    "NIFTYNXT50": "NFO",
    "SENSEX": "BFO",
    "BANKEX": "BFO",
}

STRIKE_INTERVAL_MAP = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "FINNIFTY": 50,
    "MIDCPNIFTY": 25,
    "NIFTYNXT50": 50,
    "SENSEX": 100,
    "BANKEX": 100,
}

LOT_SIZE_MAP = {
    "SENSEX": 20,
    "NIFTY": 65,
    "FINNIFTY": 60,
    "BANKNIFTY": 30,
    "MIDCPNIFTY": 120,
    "NIFTYNXT50": 25,
    "BANKEX": 30,
}

LOT_SIZES = LOT_SIZE_MAP

# ============================================================
# LOCAL FAILOVER CONFIG (MULTI-SETUP)
# ============================================================
_LOCAL_SETUP_DEFAULT = {
    "name": "DEFAULT",
    "enabled": True,
    "buy_enabled": True,
    "buy_lots": 1,
    "buy_shift_ce": 0,
    "buy_shift_pe": 0,
    "buy_fixed_exit_enabled": True,
    "buy_fixed_premium_points": 60,
    "buy_fixed_sl_enabled": False,
    "buy_sl_premium_points": 100,
    "buy_percent_exit_enabled": True,
    "buy_percent_sl_enabled": False,
    "buy_target_percent": 120,
    "buy_stoploss_percent": 60,
    "sell_enabled": True,
    "sell_lots": 1,
    "sell_shift": 100,
    "sell_shift_ce": 100,
    "sell_shift_pe": 100,
    "hedge_distance": 500,
    "hedge_distance_ce": 500,
    "hedge_distance_pe": 500,
    "sell_fixed_exit_enabled": True,
    "sell_fixed_premium_points": 60,
    "sell_fixed_sl_enabled": False,
    "sell_sl_premium_points": 100,
    "sell_percent_exit_enabled": True,
    "sell_percent_sl_enabled": False,
    "sell_target_percent": 80,
    "sell_stoploss_percent": 50,
}

LOCAL_DEFAULT = {
    "NIFTY": {
        "index_name": "NIFTY",
        "enabled": True,
        "exchange": ORDER_EXCHANGE_MAP["NIFTY"],
        "index_exchange": INDEX_EXCHANGE_MAP["NIFTY"],
        "strike_interval": STRIKE_INTERVAL_MAP["NIFTY"],
        "vix_threshold": 30,
        "expiry": None,
        "setups": [{**_LOCAL_SETUP_DEFAULT}],
    }
}

# ============================================================
# AWS
# ============================================================
dynamodb = boto3.resource("dynamodb",region_name=AWS_REGION)
default_table  = dynamodb.Table(DEFAULT_TABLE)
override_table = dynamodb.Table(OVERRIDE_TABLE)
# ============================================================
# HOLIDAY TABLE
# ============================================================
HOLIDAY_TABLE = os.getenv("HOLIDAY_TABLE", "ExchangeHolidays")
holiday_table = dynamodb.Table(HOLIDAY_TABLE)

HOLIDAY_CACHE = set()
HOLIDAY_LAST_LOAD = 0
HOLIDAY_TTL = 3600  # 1 hour cache

secrets_client = boto3.client(
    "secretsmanager",
    region_name=AWS_REGION
)

# ============================================================
# LOAD SECRETS
# ============================================================
def load_secrets():

    name=f"openalgo/{CLIENT_ID}"
    r=secrets_client.get_secret_value(SecretId=name)
    s=json.loads(r["SecretString"])

    return s["WEBHOOK_PASSKEY"],s["OPENALGO_KEY"]

PASSKEY,OPENALGO_KEY=load_secrets()

# ============================================================
# CACHE
# ============================================================
CONFIG_CACHE={}
LAST_LOAD=0
CACHE_TTL=5

def to_int(v):
    if isinstance(v,Decimal):
        return int(v)
    if isinstance(v,str) and v.isdigit():
        return int(v)
    return v

def to_number(v):

    if isinstance(v, Decimal):

        # Keep decimals like 0.8
        if v % 1 != 0:
            return float(v)

        return int(v)

    if isinstance(v,str):

        if "." in v:
            return float(v)

        if v.isdigit():
            return int(v)

    return v


# ============================================================
# RUNTIME / STATE KEYS (SETUP-AWARE)
# ============================================================
def runtime_key(index: str, setup_name: str) -> str:
    return f"{index}_{setup_name}"


def state_file(index: str, setup_name: str) -> str:
    return f"state/{index}_{setup_name}.json"


def merged_exec_context(index_cfg: dict, setup: dict) -> dict:
    ctx = {**setup}
    ctx["exchange"] = index_cfg["exchange"]
    ctx["strike_interval"] = index_cfg["strike_interval"]
    return ctx


def resolve_sell_expiry(index: str, index_cfg: dict) -> str:
    exp = index_cfg.get("expiry") if index_cfg else None
    if exp:
        return str(exp).upper().strip()
    return get_expiry(index)


def get_setup_by_name(index_cfg: dict, setup_name: str):
    for s in index_cfg.get("setups", []):
        if s.get("name") == setup_name:
            return s
    return None


def iter_index_setup_pairs(cfg_map: dict):
    for idx, index_cfg in cfg_map.items():
        for setup in index_cfg.get("setups", []):
            if setup.get("enabled", True):
                yield idx, setup.get("name", "DEFAULT"), index_cfg, setup


# ============================================================
# SETUP NORMALIZATION (MULTI-SETUP + LEGACY FLAT ROWS)
# ============================================================
def _normalize_setup(s: dict, fallback_name: str = "DEFAULT") -> dict:

    name = str(s.get("name", fallback_name))

    base = {
        "name": name,
        "enabled": s.get("enabled", True),
        "buy_enabled": s.get("buy_enabled", True),
        "buy_lots": to_int(s.get("buy_lots", 1)),
        "buy_shift_ce": to_int(s.get("buy_shift_ce", 0)),
        "buy_shift_pe": to_int(s.get("buy_shift_pe", 0)),
        "buy_fixed_exit_enabled": s.get("buy_fixed_exit_enabled", True),
        "buy_fixed_premium_points": to_number(s.get("buy_fixed_premium_points", 100)),
        "buy_fixed_sl_enabled": s.get("buy_fixed_sl_enabled", False),
        "buy_sl_premium_points": to_number(s.get("buy_sl_premium_points", 100)),
        "buy_percent_exit_enabled": s.get("buy_percent_exit_enabled", True),
        "buy_percent_sl_enabled": s.get("buy_percent_sl_enabled", False),
        "buy_target_percent": to_number(s.get("buy_target_percent", 120)),
        "buy_stoploss_percent": to_number(s.get("buy_stoploss_percent", 60)),
        "sell_enabled": s.get("sell_enabled", True),
        "sell_lots": to_int(s.get("sell_lots", 1)),
        "sell_shift": to_int(s.get("sell_shift", s.get("sell_shift_ce", 100))),
        "sell_shift_ce": to_int(s.get("sell_shift_ce", s.get("sell_shift", 100))),
        "sell_shift_pe": to_int(s.get("sell_shift_pe", s.get("sell_shift", 100))),
        "hedge_distance": to_int(
            s.get("hedge_distance", s.get("hedge_distance_ce", 500))
        ),
        "hedge_distance_ce": to_int(
            s.get("hedge_distance_ce", s.get("hedge_distance", 500))
        ),
        "hedge_distance_pe": to_int(
            s.get("hedge_distance_pe", s.get("hedge_distance", 500))
        ),
        "sell_fixed_exit_enabled": s.get("sell_fixed_exit_enabled", True),
        "sell_fixed_premium_points": to_number(s.get("sell_fixed_premium_points", 100)),
        "sell_fixed_sl_enabled": s.get("sell_fixed_sl_enabled", False),
        "sell_sl_premium_points": to_number(s.get("sell_sl_premium_points", 100)),
        "sell_percent_exit_enabled": s.get("sell_percent_exit_enabled", True),
        "sell_percent_sl_enabled": s.get("sell_percent_sl_enabled", False),
        "sell_target_percent": to_number(s.get("sell_target_percent", 80)),
        "sell_stoploss_percent": to_number(s.get("sell_stoploss_percent", 50)),
    }

    return base


def _legacy_flat_row_to_setup(item: dict) -> dict:
    return _normalize_setup(
        {
            "name": "DEFAULT",
            "enabled": item.get("enabled", True),
            "buy_enabled": item.get("buy_enabled", True),
            "buy_lots": item.get("buy_lots", 1),
            "sell_lots": item.get("sell_lots", 1),
            "sell_shift": item.get("sell_shift", 100),
            "sell_shift_ce": item.get("sell_shift_ce", item.get("sell_shift", 100)),
            "sell_shift_pe": item.get("sell_shift_pe", item.get("sell_shift", 100)),
            "buy_shift_ce": item.get("buy_shift_ce", 0),
            "buy_shift_pe": item.get("buy_shift_pe", 0),
            "hedge_distance": item.get("hedge_distance", 500),
            "hedge_distance_ce": item.get(
                "hedge_distance_ce", item.get("hedge_distance", 500)
            ),
            "hedge_distance_pe": item.get(
                "hedge_distance_pe", item.get("hedge_distance", 500)
            ),
            "buy_fixed_exit_enabled": item.get("buy_fixed_exit_enabled", True),
            "buy_fixed_premium_points": item.get("buy_fixed_premium_points", 100),
            "buy_fixed_sl_enabled": item.get("buy_fixed_sl_enabled", False),
            "buy_sl_premium_points": item.get("buy_sl_premium_points", 100),
            "buy_percent_exit_enabled": item.get("buy_percent_exit_enabled", True),
            "buy_percent_sl_enabled": item.get("buy_percent_sl_enabled", False),
            "buy_target_percent": item.get("buy_target_percent", 120),
            "buy_stoploss_percent": item.get("buy_stoploss_percent", 60),
            "sell_enabled": item.get("sell_enabled", True),
            "sell_fixed_exit_enabled": item.get("sell_fixed_exit_enabled", True),
            "sell_fixed_premium_points": item.get("sell_fixed_premium_points", 100),
            "sell_fixed_sl_enabled": item.get("sell_fixed_sl_enabled", False),
            "sell_sl_premium_points": item.get("sell_sl_premium_points", 100),
            "sell_percent_exit_enabled": item.get("sell_percent_exit_enabled", True),
            "sell_percent_sl_enabled": item.get("sell_percent_sl_enabled", False),
            "sell_target_percent": item.get("sell_target_percent", 80),
            "sell_stoploss_percent": item.get("sell_stoploss_percent", 50),
        },
        fallback_name="DEFAULT",
    )


def _apply_flat_numeric_override_to_setup(merged: dict, ov: dict, idx: str, interval: int, base_setup: dict):

    for k in [
        "buy_lots",
        "sell_lots",
        "buy_fixed_premium_points",
        "sell_fixed_premium_points",
        "buy_target_percent",
        "sell_target_percent",
        "buy_stoploss_percent",
        "sell_stoploss_percent",
        "buy_sl_premium_points",
        "sell_sl_premium_points",
        "buy_percent_exit_enabled",
        "buy_fixed_exit_enabled",
        "buy_percent_sl_enabled",
        "buy_fixed_sl_enabled",
        "sell_percent_exit_enabled",
        "sell_fixed_exit_enabled",
        "sell_percent_sl_enabled",
        "sell_fixed_sl_enabled",
        "sell_shift_ce",
        "sell_shift_pe",
        "buy_shift_ce",
        "buy_shift_pe",
    ]:
        if k in ov:
            merged[k] = to_number(ov[k])

    merged["buy_enabled"] = ov.get("buy_enabled", merged["buy_enabled"])
    merged["sell_enabled"] = ov.get("sell_enabled", merged["sell_enabled"])
    merged["enabled"] = ov.get("enabled", merged.get("enabled", True))

    if "sell_shift" in ov:
        client_shift = to_int(ov["sell_shift"])
        if client_shift % interval != 0:
            logging.error(
                f"{CLIENT_ID}-{idx} INVALID sell_shift {client_shift} "
                f"not multiple of {interval} → default {base_setup['sell_shift']} used"
            )
        else:
            merged["sell_shift"] = client_shift
            logging.info(f"{CLIENT_ID}-{idx} sell_shift overridden → {client_shift}")

    if "sell_shift_ce" in ov:
        client_shift = to_int(ov["sell_shift_ce"])
        if abs(client_shift) % interval != 0:
            logging.error(f"{CLIENT_ID}-{idx} INVALID sell_shift_ce {client_shift}")
        else:
            merged["sell_shift_ce"] = client_shift

    if "sell_shift_pe" in ov:
        client_shift = to_int(ov["sell_shift_pe"])
        if abs(client_shift) % interval != 0:
            logging.error(f"{CLIENT_ID}-{idx} INVALID sell_shift_pe {client_shift}")
        else:
            merged["sell_shift_pe"] = client_shift

    if "buy_shift_ce" in ov:
        client_shift = to_int(ov["buy_shift_ce"])
        if abs(client_shift) % interval != 0:
            logging.error(
                f"{CLIENT_ID}-{idx} INVALID buy_shift_ce {client_shift} "
                f"not multiple of {interval} → default used"
            )
        else:
            merged["buy_shift_ce"] = client_shift
            logging.info(f"{CLIENT_ID}-{idx} buy_shift_ce overridden → {client_shift}")

    if "buy_shift_pe" in ov:
        client_shift = to_int(ov["buy_shift_pe"])
        if abs(client_shift) % interval != 0:
            logging.error(
                f"{CLIENT_ID}-{idx} INVALID buy_shift_pe {client_shift} "
                f"not multiple of {interval} → default used"
            )
        else:
            merged["buy_shift_pe"] = client_shift
            logging.info(f"{CLIENT_ID}-{idx} buy_shift_pe overridden → {client_shift}")

    if "hedge_distance" in ov:
        client_hedge = to_int(ov["hedge_distance"])
        if client_hedge % interval != 0:
            logging.error(
                f"{CLIENT_ID}-{idx} INVALID hedge_distance {client_hedge} "
                f"not multiple of {interval} → default {base_setup['hedge_distance']} used"
            )
        elif client_hedge <= merged["sell_shift"]:
            logging.error(
                f"{CLIENT_ID}-{idx} INVALID hedge_distance {client_hedge} "
                f"<= sell_shift {merged['sell_shift']} → default used"
            )
        else:
            merged["hedge_distance"] = client_hedge
            logging.info(f"{CLIENT_ID}-{idx} hedge_distance overridden → {client_hedge}")

    if "hedge_distance_ce" in ov:
        val = to_int(ov["hedge_distance_ce"])
        if abs(val) % interval != 0:
            logging.error(f"INVALID hedge_distance_ce {val}")
        elif val <= abs(merged.get("sell_shift_ce", merged.get("sell_shift", 100))):
            logging.error("hedge_distance_ce too small")
        else:
            merged["hedge_distance_ce"] = val

    if "hedge_distance_pe" in ov:
        val = to_int(ov["hedge_distance_pe"])
        if abs(val) % interval != 0:
            logging.error(f"INVALID hedge_distance_pe {val}")
        elif val <= abs(merged.get("sell_shift_pe", merged.get("sell_shift", 100))):
            logging.error("hedge_distance_pe too small")
        else:
            merged["hedge_distance_pe"] = val


def _default_index_skeleton(idx: str, src: dict) -> dict:
    return {
        "index_name": idx,
        "enabled": src.get("enabled", False),
        "exchange": ORDER_EXCHANGE_MAP.get(idx, src.get("exchange", "NFO")),
        "index_exchange": INDEX_EXCHANGE_MAP.get(idx, "NSE_INDEX"),
        "strike_interval": to_int(
            src.get("strike_interval", STRIKE_INTERVAL_MAP.get(idx, 50))
        ),
        "vix_threshold": to_number(src.get("vix_threshold", 30)),
        "expiry": src.get("expiry"),
        "setups": [],
    }


def _parse_setups_from_dynamo_item(item: dict) -> list:
    if "setups" not in item:
        return [_legacy_flat_row_to_setup(item)]

    raw = item.get("setups") or []
    out = []
    for j, s in enumerate(raw):
        if isinstance(s, dict):
            out.append(_normalize_setup(s, fallback_name=s.get("name", f"SETUP{j}")))
    return out


def _merge_setup_dict(base: dict, ov: dict, idx: str, interval: int) -> dict:
    merged = dict(base)
    _apply_flat_numeric_override_to_setup(merged, ov, idx, interval, base)
    if isinstance(ov, dict) and ov.get("name"):
        merged["name"] = str(ov["name"])
    return merged


# ============================================================
# LOAD CONFIG (WITH VALIDATION) — MULTI-SETUP
# ============================================================
def load_indices_config():

    global CONFIG_CACHE, LAST_LOAD

    if time.time() - LAST_LOAD < CACHE_TTL:
        return CONFIG_CACHE

    try:

        resp = default_table.scan()
        defaults = {}

        for i in resp.get("Items", []):
            idx = i["index_name"]
            skel = _default_index_skeleton(idx, i)
            skel["setups"] = _parse_setups_from_dynamo_item(i)
            if not i.get("setups"):
                skel["enabled"] = i.get("enabled", False)
            defaults[idx] = skel

        resp2 = override_table.query(
            KeyConditionExpression=Key("client_id").eq(CLIENT_ID)
        )

        overrides = {}
        disabled = set()

        for i in resp2.get("Items", []):
            idx = i["index_name"]
            if not i.get("approved", False):
                disabled.add(idx)
                continue
            overrides[idx] = i

        final = {}

        for idx, index_cfg in defaults.items():

            if idx in disabled:
                continue

            merged_index = dict(index_cfg)
            interval = merged_index["strike_interval"]

            if idx in overrides:
                ov = overrides[idx]

                if "enabled" in ov:
                    merged_index["enabled"] = ov["enabled"]

                if "exchange" in ov:
                    merged_index["exchange"] = ov["exchange"]

                if "strike_interval" in ov:
                    merged_index["strike_interval"] = to_int(ov["strike_interval"])
                    interval = merged_index["strike_interval"]

                if "vix_threshold" in ov:
                    merged_index["vix_threshold"] = to_number(ov["vix_threshold"])

                if "expiry" in ov:
                    merged_index["expiry"] = ov["expiry"]

                ov_setups_raw = ov.get("setups")

                if ov_setups_raw:
                    by_name = {}
                    for s in ov_setups_raw:
                        if isinstance(s, dict) and s.get("name"):
                            by_name[str(s["name"])] = s

                    new_list = []
                    for s in merged_index["setups"]:
                        nm = s["name"]
                        if nm in by_name:
                            new_list.append(
                                _merge_setup_dict(s, by_name[nm], idx, interval)
                            )
                        else:
                            new_list.append(dict(s))

                    for nm, patch in by_name.items():
                        if not any(x["name"] == nm for x in new_list):
                            new_list.append(
                                _normalize_setup(patch, fallback_name=nm)
                            )

                    merged_index["setups"] = new_list

                else:

                    target = None
                    for s in merged_index["setups"]:
                        if s["name"] == "DEFAULT":
                            target = s
                            break
                    if target is None and merged_index["setups"]:
                        target = merged_index["setups"][0]

                    if target is not None:
                        ti = merged_index["setups"].index(target)
                        merged_index["setups"][ti] = dict(target)
                        _apply_flat_numeric_override_to_setup(
                            merged_index["setups"][ti], ov, idx, interval, target
                        )

            if not merged_index.get("enabled", True):
                continue

            merged_index["setups"] = [
                s for s in merged_index["setups"] if s.get("enabled", True)
            ]

            if not merged_index["setups"]:
                continue

            final[idx] = merged_index

        CONFIG_CACHE = final
        LAST_LOAD = time.time()

        return final

    except Exception as e:

        logging.error(f"DB FAIL → LOCAL CONFIG: {e}")
        return LOCAL_DEFAULT


INDICES = load_indices_config()

# ============================================================
# LOGGING
# ============================================================
os.makedirs("logs",exist_ok=True)

logging.basicConfig(
    filename="logs/bot.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger=logging.getLogger(__name__)

# ============================================================
# TIME
# ============================================================
IST=pytz.timezone("Asia/Kolkata")
def now_ist(): return datetime.now(IST)

# ============================================================
# FASTAPI / LOCKS (app constructed after lifespan)
# ============================================================
locks = defaultdict(asyncio.Lock)

# ============================================================
# OPENALGO
# ============================================================
APP_PORT = os.getenv("APP_PORT", "5000")
OPENALGO_HOST = os.getenv("OPENALGO_HOST", f"http://127.0.0.1:{APP_PORT}")

print(f"🚀 Bot connecting to OpenAlgo → {OPENALGO_HOST}")

client = api(
    api_key=OPENALGO_KEY,
    host=OPENALGO_HOST
)
#client=api(
#    api_key=OPENALGO_KEY,
#    host="http://127.0.0.1:5003"
#)

strategy="Tradingview"
product="NRML"

# ============================================================
# STATE
# ============================================================
os.makedirs("state",exist_ok=True)

def load_state(index, setup_name):

    path = state_file(index, setup_name)

    if os.path.exists(path):
        return json.load(open(path))

    if setup_name == "DEFAULT":
        legacy = f"state/{index}.json"
        if os.path.exists(legacy):
            return json.load(open(legacy))

    return {
        "position_open": False,
        "order_in_progress": False,
        "last_trade_time": 0,
        "side": None,
    }


def save_state(index, setup_name, d):
    json.dump(d, open(state_file(index, setup_name), "w"))

# ============================================================
# COOLDOWN
# ============================================================
COOLDOWN=5

def cooldown_ok(st):
    return time.time()-st["last_trade_time"]>COOLDOWN

# ============================================================
# STRIKE
# ============================================================
def ceil_strike(p,i): return int(math.ceil(p/i)*i)
def floor_strike(p,i): return int(math.floor(p/i)*i)

def round_to_tick(price, tick_size=0.05, action="BUY"):

    if action == "BUY":
        return math.ceil(price / tick_size) * tick_size
    else:
        return math.floor(price / tick_size) * tick_size
# ============================================================
# EXPIRY ENGINE
# ============================================================
def next_weekday(date, weekday):
    days_ahead = weekday - date.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return date + timedelta(days=days_ahead)

def last_weekday_of_month(date, weekday):

    if date.month == 12:
        first_next = date.replace(year=date.year+1, month=1, day=1)
    else:
        first_next = date.replace(month=date.month+1, day=1)

    last_day = first_next - timedelta(days=1)

    while last_day.weekday() != weekday:
        last_day -= timedelta(days=1)

    return last_day
# ============================================================
# HOLIDAY LOADER
# ============================================================
def load_holidays():

    global HOLIDAY_CACHE, HOLIDAY_LAST_LOAD

    if time.time() - HOLIDAY_LAST_LOAD < HOLIDAY_TTL:
        return HOLIDAY_CACHE

    try:

        resp = holiday_table.scan()
        holidays = set()

        for i in resp.get("Items", []):
            holidays.add(i["holiday_date"].upper())

        HOLIDAY_CACHE = holidays
        HOLIDAY_LAST_LOAD = time.time()

        logging.info(f"Holidays loaded → {len(holidays)} days")

        return holidays

    except Exception as e:

        logging.error(f"Holiday load failed → {e}")
        return HOLIDAY_CACHE

# ============================================================
# TRADING DAY CHECK
# ============================================================
def is_holiday(date):

    holidays = load_holidays()

    d = date.strftime("%d%b%y").upper()

    # Weekend
    if date.weekday() >= 5:
        return True

    # Exchange holiday
    if d in holidays:
        return True

    return False


def previous_trading_day(date):

    d = date

    while is_holiday(d):
        d -= timedelta(days=1)

    return d

def get_expiry(index):

    today = now_ist().date()

    if index == "NIFTY":
        expiry = next_weekday(today, 1)
        if (expiry - today).days < 4:
            expiry += timedelta(days=7)

    elif index == "SENSEX":
        expiry = next_weekday(today, 3)
        if (expiry - today).days < 2:
            expiry += timedelta(days=7)

    elif index in ["MIDCPNIFTY","FINNIFTY","NIFTYNXT50","BANKNIFTY"]:
        expiry = last_weekday_of_month(today, 1)
        if (expiry - today).days < 2:
            nm = today.replace(month=today.month%12+1, day=1)
            expiry = last_weekday_of_month(nm, 1)

    elif index in ["BANKEX"]:
        expiry = last_weekday_of_month(today, 3)
        if (expiry - today).days < 2:
            nm = today.replace(month=today.month%12+1, day=1)
            expiry = last_weekday_of_month(nm, 3)

    else:
        raise ValueError(index)
    expiry = previous_trading_day(expiry)
    return expiry.strftime("%d%b%y").upper()

# ============================================================
# NEAREST EXPIRY ENGINE — BUY ONLY
# ============================================================
def get_nearest_expiry(index):
    now = now_ist()
    today = now.date()
    now_time = now.time()

    # Configuration for expiry days (0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri)
    weekly_expiry_days = {"NIFTY": 1, "SENSEX": 3} # Adjusted SENSEX to 4 (Fri) per NSE/BSE standards
    monthly_expiry_days = {
        "FINNIFTY": 1, "MIDCPNIFTY": 1, "NIFTYNXT50": 1, 
        "BANKNIFTY": 1, "BANKEX": 3
    }

    # Market session bounds
    market_close = dt_time(15, 30)

    if index in weekly_expiry_days:
        target_weekday = weekly_expiry_days[index]
        
        # 1. Find the raw weekday for this week
        days_ahead = (target_weekday - today.weekday()) % 7
        raw_expiry = today + timedelta(days=days_ahead)
        
        # 2. Adjust for holidays
        expiry = previous_trading_day(raw_expiry)

        # 3. Handle Roll-over logic
        # If today is the expiry day and market is closed, move to NEXT week
        if today > expiry or (today == expiry and now_time > market_close):
            # Move to next week's raw day and adjust for holidays again
            next_raw = raw_expiry + timedelta(days=7)
            expiry = previous_trading_day(next_raw)

    elif index in monthly_expiry_days:
        target_weekday = monthly_expiry_days[index]
        
        # 1. Get expiry for current month
        expiry = previous_trading_day(last_weekday_of_month(today, target_weekday))

        # 2. Handle Roll-over logic
        if today > expiry or (today == expiry and now_time > market_close):
            # Calculate for next month
            if today.month == 12:
                next_month_date = today.replace(year=today.year + 1, month=1, day=1)
            else:
                next_month_date = today.replace(month=today.month + 1, day=1)
            
            expiry = previous_trading_day(last_weekday_of_month(next_month_date, target_weekday))

    else:
        raise ValueError(f"No expiry rule defined for index: {index}")

    return expiry.strftime("%y%b%d").upper()




# ============================================================
# SYMBOL
# ============================================================

# ============================================================
# SYMBOL ENGINE — ATM REFERENCED
# ============================================================
def build_spread_symbols(index, price, signal, index_cfg, cfg):

    interval = cfg["strike_interval"]

    shift_ce = cfg.get("sell_shift_ce", cfg.get("sell_shift", 100))
    shift_pe = cfg.get("sell_shift_pe", cfg.get("sell_shift", 100))

    hedge_ce = cfg.get("hedge_distance_ce", cfg.get("hedge_distance", 400))
    hedge_pe = cfg.get("hedge_distance_pe", cfg.get("hedge_distance", 400))

    if signal == "ce_sell":
        atm = ceil_strike(price, interval)

        sell_strike = atm + shift_ce
        hedge_strike = atm + hedge_ce

        opt = "CE"

    elif signal == "pe_sell":
        atm = floor_strike(price, interval)

        sell_strike = atm - shift_pe
        hedge_strike = atm - hedge_pe

        opt = "PE"

    else:
        raise ValueError(signal)

    expiry = resolve_sell_expiry(index, index_cfg)

    sell_symbol = f"{index}{expiry}{sell_strike}{opt}"
    hedge_symbol = f"{index}{expiry}{hedge_strike}{opt}"

    return sell_symbol, hedge_symbol


def build_sell_symbol(index, price, signal, index_cfg, cfg):

    if signal == "ce_sell":
        strike = ceil_strike(price, cfg["strike_interval"]) + cfg["sell_shift"]
        opt = "CE"

    elif signal == "pe_sell":
        strike = floor_strike(price, cfg["strike_interval"]) - cfg["sell_shift"]
        opt = "PE"

    else:
        raise ValueError(signal)

    return f"{index}{resolve_sell_expiry(index, index_cfg)}{strike}{opt}"

def hedge_symbol(sym, dist):

    base = sym[:-7]
    strike = int(sym[-7:-2])
    opt = sym[-2:]

    hedge = strike + dist if opt == "CE" else strike - dist
    return f"{base}{hedge}{opt}"

# ============================================================
# BUY SYMBOL ENGINE — ATM + NEAREST EXPIRY
# ============================================================
def build_buy_symbol(index, price, signal, index_cfg, cfg):

    interval = cfg["strike_interval"]

    shift_ce = cfg.get("buy_shift_ce", 0)
    shift_pe = cfg.get("buy_shift_pe", 0)

    # ---------- STRIKE BUILD ----------
    if signal == "ce_buy":

        base = ceil_strike(price, interval)
        strike = base + shift_ce
        opt = "CE"

    elif signal == "pe_buy":

        base = floor_strike(price, interval)
        strike = base - shift_pe   # IMPORTANT
        opt = "PE"

    else:
        raise ValueError(f"Invalid BUY signal → {signal}")

    # ---------- EXPIRY ----------
    expiry = get_nearest_expiry(index)

    return f"{index}{expiry}{strike}{opt}"

# ============================================================
# ORDER
# ============================================================
def place(action, symbol, index, cfg, strategy_type):

    try:
        # ---------------------------------------
        # Quantity
        # ---------------------------------------
        if strategy_type == "buy":
            lots = cfg.get("buy_lots", 1)
        else:
            lots = cfg.get("sell_lots", 1)

        qty = lots * LOT_SIZE_MAP[index]

        logger.info(f"[START] {action} {symbol} | Strategy:{strategy_type}")

        # ---------------------------------------
        # Fetch LTP
        # ---------------------------------------
        quote = client.quotes(
            symbol=symbol,
            exchange=cfg["exchange"]
        )

        logger.info(f"[QUOTE] {symbol} → {quote}")

        if quote.get("status") != "success":
            logger.error(f"[QUOTE FAILED] {symbol} → {quote}")
            raise Exception(f"Quote failed: {quote}")

        ltp = float(quote["data"]["ltp"])

        # ---------------------------------------
        # Smart Buffer
        # ---------------------------------------
        buffer = max(2, ltp * 0.05)

        if action == "BUY":
            raw_price = ltp + buffer
        else:
            raw_price = ltp - buffer

        # ---------------------------------------
        # Tick Adjust
        # ---------------------------------------
        limit_price = round_to_tick(raw_price, action=action)

        logger.info(
            f"[PRICE] {symbol} | LTP:{ltp} | Buffer:{buffer} | Raw:{raw_price} | Limit:{limit_price}"
        )

        # ---------------------------------------
        # Payload
        # ---------------------------------------
        payload = dict(
            strategy=strategy,
            symbol=symbol,
            action=action,
            exchange=cfg["exchange"],
            price_type="LIMIT",
            price=limit_price,
            product=product,
            quantity=qty
        )

        logger.info(f"[PAYLOAD] {payload}")

        # ---------------------------------------
        # Place Order
        # ---------------------------------------
        r = client.placeorder(**payload)

        logger.info(f"[RESPONSE] {symbol} → {r}")

        if r.get("status") != "success":
            logger.error(f"[ORDER FAILED] {symbol} → {r}")
            raise RuntimeError(r)

        logger.info(f"[SUCCESS] {action} {symbol} | Qty:{qty}")

        return r

    except Exception as e:
        logger.exception(f"[EXCEPTION] {action} {symbol} → {e}")
        raise
# ============================================================
# AUTH
# ============================================================
def validate_passkey(d):
    if d.get("passkey")!=PASSKEY:
        raise HTTPException(401,"Unauthorized")

# ============================================================
# SAVE TRADE TO DYNAMODB
# ============================================================

def save_trade_to_db(trade_data):
    table = dynamodb.Table("ClientTrades")
    table.put_item(Item=trade_data)


async def calculate_spread_pnl(trade, index_cfg):

    sell_quote = await asyncio.to_thread(
        client.quotes,
        symbol=trade["sell_symbol"],
        exchange=index_cfg["exchange"],
    )

    hedge_quote = await asyncio.to_thread(
        client.quotes,
        symbol=trade["hedge_symbol"],
        exchange=index_cfg["exchange"],
    )


    sell_ltp  = Decimal(str(sell_quote["data"]["ltp"]))
    hedge_ltp = Decimal(str(hedge_quote["data"]["ltp"]))

    #lot_size = LOT_SIZES[trade["index"]]

    # If you have multiple lots:
    #lots = cfg.get("sell_lots", 1)

    total_qty = Decimal(trade["quantity"])

    sell_entry  = Decimal(trade["sell_entry"])
    hedge_entry = Decimal(trade["hedge_entry"])

    sell_pnl  = (sell_entry - sell_ltp) * total_qty
    hedge_pnl = (hedge_ltp - hedge_entry) * total_qty

    total_pnl = sell_pnl + hedge_pnl

    return total_pnl

async def calculate_buy_pnl(trade, index_cfg):

    quote = await asyncio.to_thread(
        client.quotes,
        symbol=trade["symbol"],
        exchange=index_cfg["exchange"],
    )

    ltp = Decimal(str(quote["data"]["ltp"]))
    entry = Decimal(trade["entry"])
    qty = Decimal(trade["quantity"])

    return (ltp - entry) * qty


# ============================================================
# MARKET HOURS GUARD
# ============================================================

def market_open():

    now_dt = now_ist()

    # Weekend guard
    if now_dt.weekday() >= 5:
        return False

    now = now_dt.time()

    market_start = dt_time(0, 5)
    market_end   = dt_time(23, 59)

    return market_start <= now <= market_end

async def pnl_monitor():

    print("🚀 PNL Monitor Started", flush=True)

    table = dynamodb.Table("ClientTrades")

    while True:
        if not market_open():

            print(f"{now_ist()} 💤 Market closed — PNL monitor sleeping", flush=True)

            await asyncio.sleep(300)   # Sleep 5 mins
            continue


        items = []

        try:
            print(f"{now_ist()} 💓 Heartbeat", flush=True)

            resp = await asyncio.to_thread(
                table.query,
                KeyConditionExpression=Key("client_id").eq(CLIENT_ID)
            )

            items = resp.get("Items", [])

            print(f"{now_ist()} 📊 Trades found: {len(items)}", flush=True)

        except Exception as e:
            print("🔥 DynamoDB Query Error:", e, flush=True)
            await asyncio.sleep(45)
            continue

        # =========================================================
        # PROCESS TRADES
        # =========================================================
        try:
            cfg_map = load_indices_config()
            for trade in items:

                if trade.get("status") != "OPEN":
                    continue

                trade_id = trade["trade_id"]
                index    = trade["index"]
                strategy = trade.get("strategy_type")

                print(f"🔍 Checking {trade_id} | Strategy: {strategy}", flush=True)

                cfg = cfg_map.get(index)                

                if not cfg:
                    continue



                # =========================================================
                # CALCULATE ₹ PNL + PREMIUM CAPTURE (POINTS)
                # =========================================================

                premium_captured = Decimal("0")


                # -----------------------------------------------
                # SELL SPREAD
                # -----------------------------------------------
                if strategy == "sell_spread":

                    pnl = await calculate_spread_pnl(trade, cfg)

                    sell_quote = await asyncio.to_thread(
                        client.quotes,
                        symbol=trade["sell_symbol"],
                        exchange=cfg["exchange"],
                    )

                    hedge_quote = await asyncio.to_thread(
                        client.quotes,
                        symbol=trade["hedge_symbol"],
                        exchange=cfg["exchange"],
                    )

                    sell_ltp  = Decimal(str(sell_quote["data"]["ltp"]))
                    hedge_ltp = Decimal(str(hedge_quote["data"]["ltp"]))

                    sell_entry  = Decimal(trade["sell_entry"])
                    hedge_entry = Decimal(trade["hedge_entry"])

                    entry_diff   = sell_entry - hedge_entry
                    current_diff = sell_ltp - hedge_ltp

                    premium_captured = entry_diff - current_diff


                # -----------------------------------------------
                # BUY
                # -----------------------------------------------
                elif strategy == "buy":

                    pnl = await calculate_buy_pnl(trade, cfg)

                    quote = await asyncio.to_thread(
                        client.quotes,
                        symbol=trade["symbol"],
                        exchange=cfg["exchange"],
                    )

                    ltp   = Decimal(str(quote["data"]["ltp"]))
                    entry = Decimal(trade["entry"])

                    premium_captured = ltp - entry

                else:
                    continue


                # =========================================================
                # TARGET ENGINE
                # =========================================================

                expected_profit = Decimal(trade.get("expected_profit", 0))
                

                if strategy == "buy":
                    target_percent = Decimal(trade.get("buy_target_percent", 0)) / Decimal(100)
                    stoploss_percent = Decimal(trade.get("buy_stoploss_percent", 0)) / Decimal(100)
                    percent_exit_enabled = trade.get(
                        "buy_percent_exit_enabled", False
                    )

                    percent_sl_enabled = trade.get(
                        "buy_percent_sl_enabled", False
                    )

                    fixed_exit_enabled = trade.get(
                        "buy_fixed_exit_enabled", False
                    )

                    fixed_sl_enabled = trade.get(
                        "buy_fixed_sl_enabled", False
                    )

                    fixed_target = Decimal(
                        trade.get("buy_fixed_premium_points", 0)
                    )

                    fixed_sl = Decimal(
                        trade.get("buy_sl_premium_points", 0)
                    )


                elif strategy == "sell_spread":

                    target_percent = Decimal(trade.get("sell_target_percent", 0)) / Decimal(100)
                    stoploss_percent = Decimal(trade.get("sell_stoploss_percent", 0)) / Decimal(100)
                    percent_exit_enabled = trade.get(
                        "sell_percent_exit_enabled", False
                    )

                    percent_sl_enabled = trade.get(
                        "sell_percent_sl_enabled", False
                    )

                    fixed_exit_enabled = trade.get(
                        "sell_fixed_exit_enabled", False
                    )

                    fixed_sl_enabled = trade.get(
                        "sell_fixed_sl_enabled", False
                    )

                    fixed_target = Decimal(
                        trade.get("sell_fixed_premium_points", 0)
                    )

                    fixed_sl = Decimal(
                        trade.get("sell_sl_premium_points", 0)
                    )


                else:

                    target_percent = Decimal("0")
                    stoploss_percent = Decimal("0")

                    percent_exit_enabled = False
                    percent_sl_enabled = False
                    fixed_exit_enabled = False
                    fixed_sl_enabled = False

                    fixed_target = Decimal("0")
                    fixed_sl = Decimal("0")





                percent_target = expected_profit * target_percent
                percent_sl     = expected_profit * stoploss_percent
                # ---------------------------------------------------------
                # CONFIG EXIT FLAGS (FROM ClientAlgoConfig)
                # ---------------------------------------------------------




                
                # ---------------------------------
                # FIXED PREMIUM TARGET (POINTS)
                # ---------------------------------



                log_parts = [
                    f"💰 {trade_id}",
                    f"{index}",
                    f"PNL₹:{pnl}",
                    f"PremiumPts:{premium_captured}"
                ]

                if percent_exit_enabled and percent_target > 0:
                    log_parts.append(f"%Target₹:{percent_target}")

                if fixed_exit_enabled and fixed_target > 0:
                    log_parts.append(f"FixedTargetPts:{fixed_target}")

                if percent_sl_enabled and percent_sl > 0:
                    log_parts.append(f"%SL₹:{percent_sl}")

                if fixed_sl_enabled and fixed_sl > 0:
                    log_parts.append(f"FixedSLPts:{fixed_sl}")

                print(" | ".join(log_parts), flush=True)





                exit_hit = False
                reason = None

                # ---------------------------------------------------------
                # STOP LOSS CHECK (FIRST PRIORITY)
                # ---------------------------------------------------------

                if (
                    percent_sl_enabled
                    and percent_sl > 0
                    and pnl <= -percent_sl
                ):

                    exit_hit = True
                    reason = "STOPLOSS_PERCENT"


                elif (
                    fixed_sl_enabled
                    and fixed_sl > 0
                    and premium_captured <= -fixed_sl
                ):

                    exit_hit = True
                    reason = "STOPLOSS_PREMIUM"


                # ------------------------------
                # PERCENT EXIT
                # ------------------------------

                elif (
                    percent_exit_enabled
                    and percent_target > 0
                    and pnl >= percent_target
                ):

                    exit_hit = True
                    reason = "PERCENT_TARGET"


                # ------------------------------
                # FIXED PREMIUM EXIT
                # ------------------------------

                elif (
                    fixed_exit_enabled
                    and fixed_target > 0
                    and premium_captured >= fixed_target
                ):

                    exit_hit = True
                    reason = "FIXED_PREMIUM_TARGET"



                if exit_hit:

                    print(
                        f"🎯 Exit hit for {trade_id} | Reason: {reason}",
                        flush=True
                    )

                    try:

                        # -----------------------------
                        # Close Position
                        # -----------------------------
                        setup_nm = trade.get("setup_name", "DEFAULT")

                        if strategy == "sell_spread":
                            await execute_sell_exit_internal(index, setup_nm)

                        elif strategy == "buy":
                            await execute_buy_exit(index, setup_nm)

                        # -----------------------------
                        # Update DB
                        # -----------------------------
                        await asyncio.to_thread(
                            table.update_item,
                            Key={
                                "client_id": CLIENT_ID,
                                "trade_id": trade_id
                            },
                            UpdateExpression="""
                                SET #s = :closed,
                                    exit_time = :t,
                                    exit_reason = :r
                            """,
                            ExpressionAttributeNames={
                                "#s": "status"
                            },
                            ExpressionAttributeValues={
                                ":closed": "CLOSED",
                                ":t": int(time.time()),
                                ":r": reason
                            }
                        )

                    except Exception as e:
                        print("🔥 Trade Processing Error:", e, flush=True)

        except Exception as e:

            print(
                "🔥 Trade Processing Error:",
                e,
                flush=True
            )


        await asyncio.sleep(45)

async def execute_sell_exit_internal(index, setup_name="DEFAULT"):

    IND = load_indices_config()

    if index not in IND:
        return

    index_cfg = IND[index]
    setup = get_setup_by_name(index_cfg, setup_name)

    if not setup:
        return

    exec_cfg = merged_exec_context(index_cfg, setup)
    st = load_state(index, setup_name)

    if not st["position_open"]:
        return

    place("BUY", st["sell"], index, exec_cfg, "sell")
    time.sleep(0.5)
    place("SELL", st["hedge"], index, exec_cfg, "sell")

    st["position_open"] = False
    save_state(index, setup_name, st)

async def get_vix_value():

    try:
        quote = await asyncio.to_thread(
            client.quotes,
            symbol="INDIAVIX",      # ✅ NOT INDIAVIX-INDEX
            exchange="NSE_INDEX"    # ✅ NSE_INDEX correct
        )

        print("VIX RESPONSE:", quote, flush=True)

        if quote.get("status") != "success":
            return None

        return float(quote["data"]["ltp"])

    except Exception as e:
        print("❌ VIX fetch failed:", e, flush=True)
        return None


# ============================================================
# EXECUTION
# ============================================================
async def execute_sell(index, signal, price, setup_name="DEFAULT"):

    IND = load_indices_config()

    if index not in IND:
        return {"status": "blocked"}

    index_cfg = IND[index]
    setup = get_setup_by_name(index_cfg, setup_name)

    if not setup:
        return {"status": "blocked", "reason": "unknown setup"}

    if not setup.get("sell_enabled", True):
        return {"status": "blocked", "reason": "sell disabled"}

    exec_cfg = merged_exec_context(index_cfg, setup)
    st = load_state(index, setup_name)

    new_side = "CE" if signal == "ce_sell" else "PE"

    lk = runtime_key(index, setup_name)

    async with locks[lk]:

        if st["order_in_progress"]:
            return {"status": "rejected", "reason": "busy"}

        if not cooldown_ok(st):
            return {"status": "rejected", "reason": "cooldown"}

        st["order_in_progress"] = True
        save_state(index, setup_name, st)

        try:

            # ====================================================
            # BUILD NEW SPREAD SYMBOLS
            # ====================================================
            new_sell, new_hedge = build_spread_symbols(
                index,
                price,
                signal,
                index_cfg,
                exec_cfg,
            )

            # ====================================================
            # SIDE SWITCH / POSITION MANAGEMENT
            # ====================================================
            if st["position_open"]:

                old_side = st.get("side")

                # Same direction → ignore signal
                if old_side == new_side:
                    return {
                        "status": "ignored",
                        "reason": "same spread already open",
                    }

                print(f"🔄 SIDE SWITCH {old_side} → {new_side}", flush=True)

                # ---------------------------------------
                # CLOSE OLD POSITION FIRST
                # ---------------------------------------
                place("BUY", st["sell"], index, exec_cfg, "sell")
                time.sleep(0.5)

                place("SELL", st["hedge"], index, exec_cfg, "sell")

                st["position_open"] = False
                save_state(index, setup_name, st)

                # ---------------------------------------
                # CLOSE OLD DB TRADE
                # ---------------------------------------
                table = dynamodb.Table("ClientTrades")

                resp = await asyncio.to_thread(
                    table.query,
                    KeyConditionExpression=Key("client_id").eq(CLIENT_ID),
                )

                for trade in resp.get("Items", []):

                    if (
                        trade["index"] == index
                        and trade.get("setup_name", "DEFAULT") == setup_name
                        and trade["strategy_type"] == "sell_spread"
                        and trade["status"] == "OPEN"
                    ):

                        await asyncio.to_thread(
                            table.update_item,
                            Key={
                                "client_id": CLIENT_ID,
                                "trade_id": trade["trade_id"],
                            },
                            UpdateExpression="""
                                SET #s = :closed,
                                    exit_time = :t,
                                    exit_reason = :r
                            """,
                            ExpressionAttributeNames={
                                "#s": "status",
                            },
                            ExpressionAttributeValues={
                                ":closed": "CLOSED",
                                ":t": int(time.time()),
                                ":r": "SIDE_SWITCH",
                            },
                        )

            # ====================================================
            # VIX VALIDATION (AFTER POSITION CLOSE)
            # ====================================================
            vix_value = await get_vix_value()

            if vix_value is None:
                return {"status": "blocked", "reason": "VIX unavailable"}

            vix_threshold = index_cfg.get("vix_threshold", 30)

            if vix_value >= vix_threshold:
                return {
                    "status": "blocked",
                    "reason": f"VIX {vix_value} >= threshold {vix_threshold}",
                }

            print(f"✅ VIX OK → {vix_value}", flush=True)

            # ====================================================
            # OPEN NEW POSITION
            # ====================================================
            place("BUY", new_hedge, index, exec_cfg, "sell")
            time.sleep(0.5)

            place("SELL", new_sell, index, exec_cfg, "sell")

            # ----------------------------------------------------
            # UPDATE STATE
            # ----------------------------------------------------
            st.update(
                {
                    "position_open": True,
                    "sell": new_sell,
                    "hedge": new_hedge,
                    "side": new_side,
                    "last_trade_time": time.time(),
                }
            )

            # ----------------------------------------------------
            # FETCH ENTRY PRICES
            # ----------------------------------------------------
            sell_ltp = client.quotes(
                symbol=new_sell,
                exchange=exec_cfg["exchange"],
            )["data"]["ltp"]

            hedge_ltp = client.quotes(
                symbol=new_hedge,
                exchange=exec_cfg["exchange"],
            )["data"]["ltp"]

            # ----------------------------------------------------
            # TRADE METADATA
            # ----------------------------------------------------
            trade_id = f"{index}*{setup_name}*{int(time.time())}"

            lot_size = LOT_SIZE_MAP[index]
            lots = exec_cfg.get("sell_lots", 1)
            qty = lot_size * lots

            expected_profit = (
                Decimal(str(sell_ltp)) - Decimal(str(hedge_ltp))
            ) * Decimal(qty)

            # ----------------------------------------------------
            # SAVE TRADE
            # ----------------------------------------------------
            trade_data = {

                "client_id": CLIENT_ID,
                "trade_id": trade_id,
                "index": index,
                "setup_name": setup_name,
                "strategy_type": "sell_spread",

                "sell_symbol": new_sell,
                "hedge_symbol": new_hedge,

                "sell_entry": Decimal(str(sell_ltp)),
                "hedge_entry": Decimal(str(hedge_ltp)),

                "quantity": qty,

                "status": "OPEN",
                "created_at": int(time.time()),
                # -----------------------------
                # PROFIT TARGET
                # -----------------------------
                "sell_target_percent": Decimal(
                    str(exec_cfg.get("sell_target_percent", 80))
                ),

                "expected_profit": expected_profit,

                # -----------------------------
                # PREMIUM TARGET
                # -----------------------------
                "sell_fixed_premium_points": Decimal(
                    str(exec_cfg.get("sell_fixed_premium_points", 0))
                ),

                # -----------------------------
                # STOPLOSS
                # -----------------------------
                "sell_stoploss_percent": Decimal(
                    str(exec_cfg.get("sell_stoploss_percent", 0))
                ),

                "sell_sl_premium_points": Decimal(
                    str(exec_cfg.get("sell_sl_premium_points", 0))
                ),

                # -----------------------------
                # EXIT FLAGS SNAPSHOT
                # -----------------------------
                "sell_percent_exit_enabled": exec_cfg.get(
                    "sell_percent_exit_enabled", True
                ),

                "sell_fixed_exit_enabled": exec_cfg.get(
                    "sell_fixed_exit_enabled", True
                ),

                "sell_percent_sl_enabled": exec_cfg.get(
                    "sell_percent_sl_enabled", False
                ),

                "sell_fixed_sl_enabled": exec_cfg.get(
                    "sell_fixed_sl_enabled", False
                ),
            }

            save_trade_to_db(trade_data)

            save_state(index, setup_name, st)

            return {"status": "success"}

        finally:

            st["order_in_progress"] = False
            save_state(index, setup_name, st)

# ============================================================
# EXECUTE BUY
# ============================================================
async def execute_buy(index, signal, price, setup_name="DEFAULT"):

    IND = load_indices_config()

    if index not in IND:
        return {"status": "blocked"}

    index_cfg = IND[index]
    setup = get_setup_by_name(index_cfg, setup_name)

    if not setup:
        return {"status": "blocked", "reason": "unknown setup"}

    if not setup.get("buy_enabled", True):
        return {"status": "blocked", "reason": "buy disabled"}

    exec_cfg = merged_exec_context(index_cfg, setup)
    st = load_state(index, setup_name)

    new_side = "CE" if signal == "ce_buy" else "PE"

    lk = runtime_key(index, setup_name)

    async with locks[lk]:

        if st["order_in_progress"]:
            return {"status": "rejected", "reason": "busy"}

        if not cooldown_ok(st):
            return {"status": "rejected", "reason": "cooldown"}

        st["order_in_progress"] = True
        save_state(index, setup_name, st)

        try:

            # ====================================================
            # SIDE SWITCH CHECK
            # ====================================================
            if st.get("buy_position_open"):

                old_side = st.get("buy_side")

                if old_side == new_side:
                    return {
                        "status": "ignored",
                        "reason": "same side already open",
                    }

                # CLOSE OLD
                place(
                    "SELL",
                    st["buy_symbol"],
                    index,
                    exec_cfg,
                    "buy",
                )

            # ====================================================
            # OPEN NEW
            # ====================================================
            symbol = build_buy_symbol(index, price, signal, index_cfg, exec_cfg)

            place("BUY", symbol, index, exec_cfg, "buy")

            # ---------------------------------------
            # Fetch entry price (LTP)
            # ---------------------------------------
            quote = await asyncio.to_thread(
                client.quotes,
                symbol=symbol,
                exchange=exec_cfg["exchange"],
            )

            entry_ltp = Decimal(str(quote["data"]["ltp"]))

            # ---------------------------------------
            # Quantity Calculation
            # ---------------------------------------
            lot_size = LOT_SIZE_MAP[index]
            lots = exec_cfg.get("buy_lots", 1)
            qty = lot_size * lots

            # ---------------------------------------
            # Expected Profit Logic
            # Example: 100% premium capture target
            # ---------------------------------------
            expected_profit = entry_ltp * Decimal(qty)

            trade_id = f"{index}*{setup_name}*{int(time.time())}"

            trade_data = {
                "client_id": CLIENT_ID,
                "trade_id": trade_id,
                "index": index,
                "setup_name": setup_name,
                "strategy_type": "buy",
                "symbol": symbol,
                "entry": entry_ltp,
                "quantity": qty,
                "status": "OPEN",
                "created_at": int(time.time()),
                "expected_profit": expected_profit,
                # ---------------------------------
                # PREMIUM TARGET
                # ---------------------------------
                "buy_fixed_premium_points": Decimal(
                    str(exec_cfg.get("buy_fixed_premium_points", 0))
                ),

                # ---------------------------------
                # STOPLOSS
                # ---------------------------------
                "buy_target_percent": Decimal(
                    str(exec_cfg.get("buy_target_percent", 100))
                ),

                "buy_stoploss_percent": Decimal(
                    str(exec_cfg.get("buy_stoploss_percent", 50))
                ),
                "buy_sl_premium_points": Decimal(
                    str(exec_cfg.get("buy_sl_premium_points", 0))
                ),

                # ---------------------------------
                # EXIT FLAGS
                # ---------------------------------
                "buy_percent_exit_enabled": exec_cfg.get(
                    "buy_percent_exit_enabled", True
                ),

                "buy_fixed_exit_enabled": exec_cfg.get(
                    "buy_fixed_exit_enabled", False
                ),

                "buy_percent_sl_enabled": exec_cfg.get(
                    "buy_percent_sl_enabled", False
                ),

                "buy_fixed_sl_enabled": exec_cfg.get(
                    "buy_fixed_sl_enabled", False
                ),
            }

            save_trade_to_db(trade_data)

            st.update(
                {
                    "buy_position_open": True,
                    "buy_symbol": symbol,
                    "buy_side": new_side,
                    "last_trade_time": time.time(),
                    "active_buy_trade_id": trade_id,
                }
            )

            save_state(index, setup_name, st)

            return {"status": "success", "symbol": symbol}

        finally:
            st["order_in_progress"] = False
            save_state(index, setup_name, st)
# ============================================================
# EXECUTE BUY EXIT
# ============================================================
async def execute_buy_exit(index, setup_name="DEFAULT"):

    IND = load_indices_config()

    if index not in IND:
        return {"status": "disabled"}

    index_cfg = IND[index]
    setup = get_setup_by_name(index_cfg, setup_name)

    if not setup:
        return {"status": "disabled"}

    exec_cfg = merged_exec_context(index_cfg, setup)
    st = load_state(index, setup_name)

    lk = runtime_key(index, setup_name)

    async with locks[lk]:

        # ---------- POSITION CHECK ----------
        if not st.get("buy_position_open"):
            return {"status": "no buy position"}

        # ---------- EXIT ORDER ----------
        place("SELL", st["buy_symbol"], index, exec_cfg, "buy")

        # ---------- UPDATE STATE ----------
        st["buy_position_open"] = False
        save_state(index, setup_name, st)

    return {"status": "buy closed"}

def extract_expiry_from_symbol(index, symbol):
    """
    Extract expiry date from symbol.
    Example: NIFTY24FEB2625450PE → 24FEB26
    """
    expiry_str = symbol[len(index):len(index)+7]
    return datetime.strptime(expiry_str, "%d%b%y").date()



def validate_state_expiry(index, setup_name="DEFAULT"):

    st = load_state(index, setup_name)
    today = now_ist().date()

    table = dynamodb.Table("ClientTrades")
    changed = False

    # =====================================================
    # SELL POSITION VALIDATION
    # =====================================================
    if st.get("position_open") and st.get("sell"):

        try:
            expiry_date = extract_expiry_from_symbol(index, st["sell"])

            if today > expiry_date:

                print(
                    f"⚠️ SELL expired for {index} [{setup_name}]. Closing state + DB.",
                    flush=True,
                )

                # -------- Reset Local State --------
                st["position_open"] = False
                st["sell"] = None
                st["hedge"] = None
                st["side"] = None
                changed = True

                # -------- Close DB Trades --------
                resp = table.query(
                    KeyConditionExpression=Key("client_id").eq(CLIENT_ID),
                )

                for trade in resp.get("Items", []):

                    if (
                        trade.get("index") == index
                        and trade.get("setup_name", "DEFAULT") == setup_name
                        and trade.get("strategy_type") == "sell_spread"
                        and trade.get("status") == "OPEN"
                    ):

                        table.update_item(
                            Key={
                                "client_id": CLIENT_ID,
                                "trade_id": trade["trade_id"]
                            },
                            UpdateExpression="""
                                SET #s = :closed,
                                    exit_time = :t,
                                    exit_reason = :r,
                                    realized_pnl = :p
                            """,
                            ExpressionAttributeNames={
                                "#s": "status"
                            },
                            ExpressionAttributeValues={
                                ":closed": "CLOSED",
                                ":t": int(time.time()),
                                ":r": "EXPIRY",
                                ":p": Decimal("0")
                            }
                        )

        except Exception as e:
            print(f"🔥 SELL expiry validation error: {e}", flush=True)

    # =====================================================
    # BUY POSITION VALIDATION
    # =====================================================
    if st.get("buy_position_open") and st.get("buy_symbol"):

        try:
            expiry_date = extract_expiry_from_symbol(index, st["buy_symbol"])

            if today > expiry_date:

                print(
                    f"⚠️ BUY expired for {index} [{setup_name}]. Closing state + DB.",
                    flush=True,
                )

                # -------- Reset Local State --------
                st["buy_position_open"] = False
                st["buy_symbol"] = None
                st["buy_side"] = None
                changed = True

                # -------- Close DB Trades --------
                resp = table.query(
                    KeyConditionExpression=Key("client_id").eq(CLIENT_ID),
                )

                for trade in resp.get("Items", []):

                    if (
                        trade.get("index") == index
                        and trade.get("setup_name", "DEFAULT") == setup_name
                        and trade.get("strategy_type") == "buy"
                        and trade.get("status") == "OPEN"
                    ):

                        table.update_item(
                            Key={
                                "client_id": CLIENT_ID,
                                "trade_id": trade["trade_id"]
                            },
                            UpdateExpression="""
                                SET #s = :closed,
                                    exit_time = :t,
                                    exit_reason = :r,
                                    realized_pnl = :p
                            """,
                            ExpressionAttributeNames={
                                "#s": "status"
                            },
                            ExpressionAttributeValues={
                                ":closed": "CLOSED",
                                ":t": int(time.time()),
                                ":r": "EXPIRY",
                                ":p": Decimal("0")
                            }
                        )

        except Exception as e:
            print(f"🔥 BUY expiry validation error: {e}", flush=True)

    if changed:
        save_state(index, setup_name, st)

async def daily_expiry_validator():

    print("📅 Daily expiry validator started", flush=True)

    last_run_date = None

    while True:

        now = now_ist()

        run_time = now.replace(hour=9, minute=5, second=0, microsecond=0)

        if now >= run_time and last_run_date != now.date():

            print("⏰ Running expiry validation...", flush=True)

            for idx, setup_name, _, _ in iter_index_setup_pairs(load_indices_config()):
                validate_state_expiry(idx, setup_name)

            last_run_date = now.date()

        await asyncio.sleep(30)

@asynccontextmanager
async def lifespan(app: FastAPI):

    print("🚀 Starting Services...")

    # Run once at startup (safety)
    for idx, setup_name, _, _ in iter_index_setup_pairs(load_indices_config()):
        validate_state_expiry(idx, setup_name)

    # Start background services
    asyncio.create_task(pnl_monitor())
    asyncio.create_task(daily_expiry_validator())  # ⭐ THIS WAS MISSING

    yield

    print("🛑 Shutting down services...")


app = FastAPI(lifespan=lifespan)

# ============================================================
# SELL API
# ============================================================
@app.post("/sell")
async def sell(req: Request):

    d = await req.json()
    validate_passkey(d)

    return await execute_sell(
        d["index"],
        d["signal"],
        float(d["price"]),
        d.get("setup_name", "DEFAULT"),
    )

# ============================================================
# EXIT API
# ============================================================
@app.post("/sell_exit")
async def exit_trade(req: Request):

    d = await req.json()
    validate_passkey(d)

    index = d["index"]
    setup_name = d.get("setup_name", "DEFAULT")

    IND = load_indices_config()
    if index not in IND:
        return {"status": "disabled"}

    index_cfg = IND[index]
    setup = get_setup_by_name(index_cfg, setup_name)
    if not setup:
        return {"status": "disabled"}

    exec_cfg = merged_exec_context(index_cfg, setup)
    st = load_state(index, setup_name)

    async with locks[runtime_key(index, setup_name)]:

        if not st["position_open"]:
            return {"status": "no position"}

        # Exit orders
        place("BUY", st["sell"], index, exec_cfg, "sell")
        time.sleep(0.5)
        place("SELL", st["hedge"], index, exec_cfg, "sell")

        st["position_open"] = False
        save_state(index, setup_name, st)

        # 🔥 UPDATE DYNAMODB
        table = dynamodb.Table("ClientTrades")

        resp = table.query(
            KeyConditionExpression=Key("client_id").eq(CLIENT_ID),
        )

        for trade in resp.get("Items", []):
            if (
                trade["index"] == index
                and trade.get("setup_name", "DEFAULT") == setup_name
                and trade["status"] == "OPEN"
                and trade.get("strategy_type") == "sell_spread"
            ):
                table.update_item(
                    Key={
                        "client_id": CLIENT_ID,
                        "trade_id": trade["trade_id"],
                    },
                    UpdateExpression="SET #s = :val",
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={":val": "CLOSED"},
                )

    return {"status": "closed"}

# ============================================================
# BUY API
# ============================================================
@app.post("/buy")
async def buy(req: Request):

    d = await req.json()
    validate_passkey(d)

    return await execute_buy(
        d["index"],
        d["signal"],
        float(d["price"]),
        d.get("setup_name", "DEFAULT"),
    )

# ============================================================
# BUY EXIT API
# ============================================================
@app.post("/buy_exit")
async def buy_exit(req: Request):

    d = await req.json()
    validate_passkey(d)

    index = d["index"]
    setup_name = d.get("setup_name", "DEFAULT")

    IND = load_indices_config()

    if index not in IND:
        return {"status": "disabled"}

    index_cfg = IND[index]
    setup = get_setup_by_name(index_cfg, setup_name)
    if not setup:
        return {"status": "disabled"}

    exec_cfg = merged_exec_context(index_cfg, setup)
    st = load_state(index, setup_name)

    async with locks[runtime_key(index, setup_name)]:

        if not st.get("buy_position_open"):
            return {"status": "no buy position"}

        # Exit BUY → SELL same symbol
        place("SELL", st["buy_symbol"], index, exec_cfg, "buy")

        st["buy_position_open"] = False
        save_state(index, setup_name, st)

    return {"status": "buy closed"}

# ============================================================


# ============================================================
# LIFESPAN EVENT (RECOMMENDED)
# ============================================================



# ============================================================
@app.get("/")
def health():
    return {"status":"running"}

# ============================================================
if __name__=="__main__":
    port=int(os.getenv("BOT_PORT",7000))
    uvicorn.run(app, host="0.0.0.0", port=port, workers=1)
