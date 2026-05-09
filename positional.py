# ============================================================
# PYTRADER POSITIONAL ENGINE — SAFE VERSION
# - Retry-safe
# - Idempotent webhook protection
# - Proper exception handling
# - Prevent duplicate BUYs
# - Order response validation
# - DynamoDB safe Decimal handling
# ============================================================

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

import uvicorn
import os
import time
import asyncio
import json
import uuid

from decimal import Decimal
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

import boto3
from botocore.exceptions import ClientError
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

config_table = dynamodb.Table("ClientConfig")
symbol_table = dynamodb.Table("ClientSymbolSettings")
pos_table = dynamodb.Table("ClientPositions")
monthly_table = dynamodb.Table("ClientMonthlyUsage")

# NEW
webhook_table = dynamodb.Table("ClientWebhookHistory")

# ============================================================
# SECRETS
# ============================================================

def load_secrets():
    name = f"openalgo/{CLIENT_ID}"

    r = secrets_client.get_secret_value(SecretId=name)
    s = json.loads(r["SecretString"])

    return s["WEBHOOK_PASSKEY"], s["OPENALGO_KEY"]

PASSKEY, OPENALGO_KEY = load_secrets()

client = api(
    api_key=OPENALGO_KEY,
    host=OPENALGO_HOST
)

# ============================================================
# APP
# ============================================================

app = FastAPI()
locks = defaultdict(asyncio.Lock)

# ============================================================
# HELPERS
# ============================================================

def D(x):
    return Decimal(str(x))

def now_ts():
    return int(time.time())

def get_month_key():
    t = time.localtime()
    return f"{t.tm_year}-{str(t.tm_mon).zfill(2)}"

# ============================================================
# GLOBAL ERROR HANDLER
# ============================================================

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):

    print("❌ GLOBAL ERROR:", str(exc))

    return JSONResponse(
        status_code=200,
        content={
            "status": "error",
            "message": str(exc)
        }
    )

# ============================================================
# MONTH RESET
# ============================================================

def handle_month_reset(pos, symbol):

    current = get_month_key()

    if pos.get("month_key") != current:

        print(f"🆕 MONTH RESET → {symbol}")

        pos_table.update_item(
            Key={
                "client_id": CLIENT_ID,
                "symbol": symbol
            },
            UpdateExpression="""
                SET buy_count=:b,
                    month_key=:m,
                    tp_done=:t
            """,
            ExpressionAttributeValues={
                ":b": 0,
                ":m": current,
                ":t": []
            }
        )

        pos["buy_count"] = 0
        pos["month_key"] = current
        pos["tp_done"] = []

    return pos

# ============================================================
# VALIDATION
# ============================================================

def validate_passkey(d):

    if d.get("passkey") != PASSKEY:
        raise HTTPException(401, "Invalid passkey")

# ============================================================
# GET SYMBOL
# ============================================================

def get_symbol(symbol):

    return symbol_table.get_item(
        Key={
            "client_id": CLIENT_ID,
            "symbol": symbol
        }
    ).get("Item")

# ============================================================
# GET POSITION
# ============================================================

def get_position(symbol):

    return pos_table.get_item(
        Key={
            "client_id": CLIENT_ID,
            "symbol": symbol
        }
    ).get("Item")

# ============================================================
# LTP
# ============================================================

def get_ltp(symbol, exchange):

    res = client.quotes(
        symbol=symbol,
        exchange=exchange
    )

    if res.get("status") == "success":

        ltp = res["data"]["ltp"]

        return D(ltp)

    raise Exception(f"LTP failed → {res}")

# ============================================================
# PLACE ORDER
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

    print("📥 ORDER RESPONSE:", res)

    # IMPORTANT
    if res.get("status") != "success":
        raise Exception(f"Order failed → {res}")

    return res

# ============================================================
# PNL
# ============================================================

def calculate_pnl(ltp, pos):

    avg = D(pos["avg_price"])

    return ((ltp - avg) / avg) * 100

# ============================================================
# GLOBAL LIMIT
# ============================================================

def get_global_config():

    return config_table.get_item(
        Key={
            "client_id": CLIENT_ID,
            "config_key": "POSITIONAL"
        }
    ).get("Item", {})

# ============================================================
# MONTH LIMIT
# ============================================================

def check_monthly(amount):

    limit = D(
        get_global_config().get("monthly_limit", 0)
    )

    r = monthly_table.get_item(
        Key={
            "client_id": CLIENT_ID,
            "month_key": get_month_key()
        }
    )

    used = D(
        r.get("Item", {}).get("used_amount", 0)
    )

    return (limit <= 0) or ((used + amount) <= limit)

# ============================================================
# UPDATE MONTHLY
# ============================================================

def update_monthly(amount):

    monthly_table.update_item(
        Key={
            "client_id": CLIENT_ID,
            "month_key": get_month_key()
        },
        UpdateExpression="""
            SET used_amount =
            if_not_exists(used_amount,:z) + :a
        """,
        ExpressionAttributeValues={
            ":a": D(amount),
            ":z": D(0)
        }
    )

# ============================================================
# SYMBOL LIMIT
# ============================================================

def check_symbol_monthly(symbol, amount):

    sym = get_symbol(symbol)

    limit = D(sym.get("monthly_limit", 0))

    if limit <= 0:
        return True

    r = monthly_table.get_item(
        Key={
            "client_id": CLIENT_ID,
            "month_key": get_month_key()
        }
    )

    item = r.get("Item", {})
    symbols = item.get("symbols", {})

    used = D(symbols.get(symbol, 0))

    print(
        f"📊 SYMBOL LIMIT → "
        f"{symbol} | used={used} | "
        f"new={amount} | limit={limit}"
    )

    return (used + amount) <= limit

# ============================================================
# UPDATE SYMBOL MONTHLY
# ============================================================

def update_symbol_monthly(symbol, amount):

    monthly_table.update_item(
        Key={
            "client_id": CLIENT_ID,
            "month_key": get_month_key()
        },
        UpdateExpression="""
            SET symbols.#s =
            if_not_exists(symbols.#s, :z) + :a
        """,
        ExpressionAttributeNames={
            "#s": symbol
        },
        ExpressionAttributeValues={
            ":a": D(amount),
            ":z": D(0)
        }
    )

# ============================================================
# IDEMPOTENCY
# ============================================================

def is_duplicate(webhook_id):

    try:

        webhook_table.put_item(
            Item={
                "client_id": CLIENT_ID,
                "webhook_id": webhook_id,
                "created_at": now_ts()
            },
            ConditionExpression="""
                attribute_not_exists(webhook_id)
            """
        )

        return False

    except ClientError as e:

        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return True

        raise

# ============================================================
# BUY
# ============================================================

async def buy(symbol, webhook_id):

    sym = get_symbol(symbol)

    if not sym:
        return {"status": "symbol_not_found"}

    if not sym.get("enabled"):
        return {"status": "blocked"}

    async with locks[symbol]:

        # DUPLICATE PROTECTION
        if is_duplicate(webhook_id):

            print(f"⚠️ DUPLICATE WEBHOOK → {webhook_id}")

            return {
                "status": "duplicate_ignored"
            }

        pos = get_position(symbol)

        if pos:
            pos = handle_month_reset(pos, symbol)

        if pos and pos.get("buy_count", 0) >= sym.get("max_buys", 5):

            return {
                "status": "max_buys_reached"
            }

        # LTP
        ltp = get_ltp(
            symbol,
            sym["exchange"]
        )

        qty = int(
            D(sym["buy_amount"]) / ltp
        )

        if qty <= 0:
            return {"status": "invalid_qty"}

        invest = ltp * D(qty)

        # GLOBAL LIMIT
        if not check_monthly(invest):

            return {
                "status": "global_limit_reached"
            }

        # SYMBOL LIMIT
        if not check_symbol_monthly(symbol, invest):

            return {
                "status": "symbol_limit_reached"
            }

        # CALCULATE POSITION
        new_qty = qty if not pos else (
            int(pos["total_qty"]) + qty
        )

        if not pos:

            new_avg = ltp

        else:

            old_qty = D(pos["total_qty"])
            old_avg = D(pos["avg_price"])

            new_avg = (
                (old_qty * old_avg) +
                (D(qty) * ltp)
            ) / D(new_qty)

        # ====================================================
        # PLACE ORDER FIRST
        # ====================================================

        order_res = place(
            "BUY",
            symbol,
            sym["product"],
            sym["exchange"],
            qty
        )

        # ====================================================
        # ONLY UPDATE DB AFTER SUCCESS
        # ====================================================

        pos_table.put_item(
            Item={
                "client_id": CLIENT_ID,
                "symbol": symbol,
                "status": "OPEN",
                "total_qty": int(new_qty),
                "avg_price": D(new_avg),
                "buy_count": (
                    pos.get("buy_count", 0) + 1
                    if pos else 1
                ),
                "month_key": get_month_key(),
                "tp_done": [],
                "updated_at": now_ts()
            }
        )

        update_monthly(invest)

        update_symbol_monthly(
            symbol,
            invest
        )

        print(
            f"✅ BUY SUCCESS → "
            f"{symbol} | qty={qty} | avg={new_avg}"
        )

        return {
            "status": "success",
            "symbol": symbol,
            "qty": qty,
            "price": float(ltp),
            "webhook_id": webhook_id,
            "order_response": order_res
        }

# ============================================================
# SELL
# ============================================================

async def sell(symbol):

    sym = get_symbol(symbol)

    if not sym:
        return {"status": "symbol_not_found"}

    pos = get_position(symbol)

    if not pos:
        return {"status": "no_position"}

    ltp = get_ltp(
        symbol,
        sym["exchange"]
    )

    pnl = calculate_pnl(
        ltp,
        pos
    )

    total = int(pos["total_qty"])

    mode = sym.get("sell_mode", "IGNORE")

    # ========================================================
    # FULL EXIT
    # ========================================================

    if mode == "FULL":

        tp_full = D(sym.get("tp_full", 0))

        if pnl < tp_full:

            return {
                "status": "waiting_full_tp",
                "pnl": float(pnl)
            }

        place(
            "SELL",
            symbol,
            sym["product"],
            sym["exchange"],
            total
        )

        pos_table.update_item(
            Key={
                "client_id": CLIENT_ID,
                "symbol": symbol
            },
            UpdateExpression="""
                SET total_qty=:q,
                    buy_count=:b,
                    #s=:c
            """,
            ExpressionAttributeNames={
                "#s": "status"
            },
            ExpressionAttributeValues={
                ":q": 0,
                ":b": 0,
                ":c": "CLOSED"
            }
        )

        return {
            "status": "full_exit",
            "pnl": float(pnl)
        }

    return {
        "status": "ignored"
    }

# ============================================================
# API
# ============================================================

@app.post("/positional_buy")
async def positional_buy(req: Request):

    d = await req.json()

    validate_passkey(d)

    symbol = d.get("symbol")

    if not symbol:
        raise HTTPException(400, "symbol missing")

    # IMPORTANT
    webhook_id = d.get("webhook_id")

    if not webhook_id:

        webhook_id = str(uuid.uuid4())

    return await buy(
        symbol,
        webhook_id
    )

# ============================================================
# SELL API
# ============================================================

@app.post("/positional_sell")
async def positional_sell(req: Request):

    d = await req.json()

    validate_passkey(d)

    symbol = d.get("symbol")

    if not symbol:
        raise HTTPException(400, "symbol missing")

    return await sell(symbol)

# ============================================================
# HEALTH
# ============================================================

@app.get("/")
async def health():

    return {
        "status": "running",
        "client_id": CLIENT_ID,
        "time": now_ts()
    }

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8008
    )
