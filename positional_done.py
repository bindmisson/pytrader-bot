# ============================================================
# PYTRADER POSITIONAL ENGINE (FINAL FIXED - MARKET ONLY)
# ============================================================

from fastapi import FastAPI, Request, HTTPException
import uvicorn, os, time, asyncio, json
from decimal import Decimal
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

import boto3
from openalgo import api

# ============================================================
# ENV
# ============================================================
CLIENT_ID = os.getenv("CLIENT_ID")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
APP_PORT = int(os.getenv("APP_PORT", 5035))
OPENALGO_HOST = os.getenv("OPENALGO_HOST", f"http://127.0.0.1:{APP_PORT}")

if not CLIENT_ID:
    raise ValueError("CLIENT_ID missing")

# ============================================================
# AWS
# ============================================================
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)

config_table  = dynamodb.Table("ClientConfig")
symbol_table  = dynamodb.Table("ClientSymbolSettings")
pos_table     = dynamodb.Table("ClientPositions")
ledger_table  = dynamodb.Table("ClientPositionLedger")
monthly_table = dynamodb.Table("ClientMonthlyUsage")

# ============================================================
# SECRETS CACHE
# ============================================================
SECRETS_CACHE = None
SECRETS_LAST_LOAD = 0
SECRETS_TTL = 300

def load_secrets():
    global SECRETS_CACHE, SECRETS_LAST_LOAD

    if SECRETS_CACHE and (time.time() - SECRETS_LAST_LOAD < SECRETS_TTL):
        return SECRETS_CACHE

    name = f"openalgo/{CLIENT_ID}"
    r = secrets_client.get_secret_value(SecretId=name)
    s = json.loads(r["SecretString"])

    PASSKEY = s["WEBHOOK_PASSKEY"]
    OPENALGO_KEY = s["OPENALGO_KEY"]

    SECRETS_CACHE = (PASSKEY, OPENALGO_KEY)
    SECRETS_LAST_LOAD = time.time()

    print("🔐 Secrets loaded")
    return SECRETS_CACHE

PASSKEY, OPENALGO_KEY = load_secrets()

# ============================================================
# OPENALGO
# ============================================================
client = api(api_key=OPENALGO_KEY, host=OPENALGO_HOST)

# ============================================================
# APP
# ============================================================
app = FastAPI()
locks = defaultdict(asyncio.Lock)

# ============================================================
# HELPERS
# ============================================================
def D(x): return Decimal(str(x))

def now_ts():
    return int(time.time())

def get_month_key():
    t = time.localtime()
    return f"{t.tm_year}-{str(t.tm_mon).zfill(2)}"

def validate_passkey(d):
    if d.get("passkey") != PASSKEY:
        raise HTTPException(401, "Invalid passkey")

def get_symbol(symbol):
    return symbol_table.get_item(
        Key={"client_id": CLIENT_ID, "symbol": symbol}
    ).get("Item")

def get_position(symbol):
    return pos_table.get_item(
        Key={"client_id": CLIENT_ID, "symbol": symbol}
    ).get("Item")

# ============================================================
# LTP FROM OPENALGO
# ============================================================

def get_ltp(symbol, exchange):

    try:
        print(f"📡 Fetching LTP → symbol={symbol}, exchange={exchange}")

        res = client.quotes(symbol=symbol, exchange=exchange)

        print("🔍 FULL LTP RESPONSE:", json.dumps(res, indent=2))

        if not res:
            raise Exception("Empty response from OpenAlgo")

        # CASE 1: Standard format
        if res.get("status") == "success":
            data = res.get("data", {})

            if "ltp" in data:
                print(f"✅ LTP FOUND: {data['ltp']}")
                return float(data["ltp"])

        # CASE 2: Alternate format
        if "ltp" in res:
            print(f"✅ LTP FOUND (fallback): {res['ltp']}")
            return float(res["ltp"])

        # FAILURE CASE
        raise Exception(f"LTP fetch failed → {res}")

    except Exception as e:
        print("❌ LTP ERROR:", str(e))
        raise

# ============================================================
# ORDER
# ============================================================
def place(action, symbol, product, exchange, qty):
    payload = {
        "symbol": symbol,
        "exchange": exchange,
        "action": action,
        "product": product,
        "quantity": int(qty)
    }
    print("📤 ORDER:", payload)
    res = client.placeorder(**payload)
    print("📥 RESPONSE:", res)

    return res

# ============================================================
# CONFIG
# ============================================================
def get_global_config():
    resp = config_table.get_item(
        Key={"client_id": CLIENT_ID, "config_key": "POSITIONAL"}
    )
    return resp.get("Item", {})

# ============================================================
# MONTHLY CONTROL
# ============================================================
def check_monthly(amount):

    cfg = get_global_config()
    limit = D(cfg.get("monthly_limit", 0))

    r = monthly_table.get_item(
        Key={"client_id": CLIENT_ID, "month_key": get_month_key()}
    )

    used = D(r.get("Item", {}).get("used_amount", 0))

    if limit <= 0:
        return True

    return (used + amount) <= limit

def update_monthly(amount):

    monthly_table.update_item(
        Key={"client_id": CLIENT_ID, "month_key": get_month_key()},
        UpdateExpression="""
            SET used_amount = if_not_exists(used_amount,:z) + :a
        """,
        ExpressionAttributeValues={
            ":a": D(amount),
            ":z": D(0)
        }
    )

# ============================================================
# LEDGER
# ============================================================
def write_ledger(symbol, typ, qty, price, rem, avg):

    ledger_table.put_item(
        Item={
            "client_id": CLIENT_ID,
            "event_id": f"{symbol}_{int(time.time()*1000)}",
            "symbol": symbol,
            "event_type": typ,
            "qty": int(qty),
            "price": D(price),
            "amount": D(price) * D(qty),
            "remaining_qty": D(rem),
            "avg_price": D(avg),
            "created_at": now_ts()
        }
    )

# ============================================================
# BUY ENGINE (FIXED)
# ============================================================
async def buy(symbol):

    sym = get_symbol(symbol)
    if not sym or not sym.get("enabled"):
        return {"status": "blocked"}

    ltp = get_ltp(symbol, sym["exchange"])

    buy_amount = Decimal(str(sym.get("buy_amount", 0)))
    ltp = Decimal(str(ltp))

    qty = int(buy_amount / ltp)    

    if qty <= 0:
        return {"status": "invalid qty"}

    async with locks[symbol]:

        pos = get_position(symbol)

        if pos and pos.get("buy_count", 0) >= sym.get("max_buys", 5):
            return {"status": "max buys reached"}

        invest = D(qty) * D(ltp)

        if not check_monthly(invest):
            return {"status": "monthly limit reached"}

        place("BUY", symbol, sym["product"], sym["exchange"], qty)

        new_qty = qty if not pos else pos["total_qty"] + qty
        new_avg = ltp if not pos else (
            (pos["total_qty"] * pos["avg_price"] + qty * ltp) / new_qty
        )

        pos_table.put_item(
            Item={
                "client_id": CLIENT_ID,
                "symbol": symbol,
                "status": "OPEN",
                "total_qty": int(new_qty),
                "avg_price": D(new_avg),
                "invested_amount": D(new_qty) * D(new_avg),
                "buy_count": (pos.get("buy_count", 0) + 1 if pos else 1)
            }
        )

        write_ledger(symbol, "BUY", qty, ltp, new_qty, new_avg)
        update_monthly(invest)

        return {"status": "success", "qty": qty}

# ============================================================
# SELL ENGINE
# ============================================================
async def sell(symbol):

    sym = get_symbol(symbol)
    pos = get_position(symbol)

    if not pos:
        return {"status": "no position"}

    mode = sym.get("sell_mode", "IGNORE")

    if mode == "IGNORE":
        return {"status": "ignored"}

    total = int(pos["total_qty"])

    if mode == "FULL":
        qty = total
    else:
        qty = max(int(total * sym.get("sell_percent", 5) / 100), 1)

    place("SELL", symbol, sym["product"], sym["exchange"], qty)

    remaining = total - qty

    if remaining <= 0:
        pos_table.update_item(
            Key={"client_id": CLIENT_ID, "symbol": symbol},
            UpdateExpression="SET #s=:c",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":c": "CLOSED"}
        )
    else:
        pos_table.update_item(
            Key={"client_id": CLIENT_ID, "symbol": symbol},
            UpdateExpression="SET total_qty=:q",
            ExpressionAttributeValues={":q": remaining}
        )

    write_ledger(symbol, "SELL", qty, pos["avg_price"], remaining, pos["avg_price"])

    return {"status": "done"}

# ============================================================
# API (FIXED)
# ============================================================
@app.post("/positional_buy")
async def positional_buy(req: Request):
    d = await req.json()
    validate_passkey(d)

    if "symbol" not in d:
        raise HTTPException(400, "symbol missing")

    return await buy(d["symbol"])


@app.post("/positional_sell")
async def positional_sell(req: Request):
    d = await req.json()
    validate_passkey(d)

    if "symbol" not in d:
        raise HTTPException(400, "symbol missing")

    return await sell(d["symbol"])

# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8008)
