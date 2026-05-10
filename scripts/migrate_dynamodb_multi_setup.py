#!/usr/bin/env python3
"""
Migrate DynamoDB items to the multi-setup schema expected by sell.py.

What this does
--------------
1) DefaultAlgoConfig / ClientAlgoConfig (override)
   - If items have no non-empty `setups` list, folds legacy flat strategy fields
     into `setups: [{ "name": "DEFAULT", ... }]`.
   - Keeps index-level attributes at the root: index_name, enabled, exchange,
     strike_interval, vix_threshold, expiry (+ client_id, approved on overrides).

2) ClientTrades
   - Sets `setup_name` to "DEFAULT" when missing (backward compatibility with
     trades opened before multi-setup).

Environment (same as bot): AWS_REGION, CLIENT_ID, DEFAULT_TABLE, OVERRIDE_TABLE.

**AWS_REGION** — must match the region where your DynamoDB tables live.
Production stacks commonly use **`AWS_REGION=ap-east-1`** (Asia Pacific — Hong Kong).
If unset, this script defaults to **ap-south-1** (same fallback pattern as `sell.py`).

Usage
-----
  AWS_REGION=ap-east-1 python scripts/migrate_dynamodb_multi_setup.py --dry-run
  AWS_REGION=ap-east-1 python scripts/migrate_dynamodb_multi_setup.py --defaults --overrides --trades
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

# Match region to your tables (see module docstring: e.g. AWS_REGION=ap-east-1).
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
CLIENT_ID = os.getenv("CLIENT_ID")
DEFAULT_TABLE = os.getenv("DEFAULT_TABLE", "DefaultAlgoConfig")
OVERRIDE_TABLE = os.getenv("OVERRIDE_TABLE", "ClientAlgoConfig")
TRADES_TABLE = "ClientTrades"

# Root attributes kept on config rows (everything else becomes DEFAULT setup fields).
_DEFAULT_ROOT_KEYS = frozenset(
    {
        "index_name",
        "enabled",
        "exchange",
        "strike_interval",
        "vix_threshold",
        "expiry",
    }
)

_OVERRIDE_EXTRA_ROOT_KEYS = frozenset({"client_id", "approved"})


def _needs_setup_migration(item: dict[str, Any]) -> bool:
    setups = item.get("setups")
    if setups is None:
        return True
    if isinstance(setups, list) and len(setups) == 0:
        return True
    return False


def _migrate_config_item(item: dict[str, Any], *, override_row: bool) -> dict[str, Any]:
    reserved = set(_DEFAULT_ROOT_KEYS)
    if override_row:
        reserved |= _OVERRIDE_EXTRA_ROOT_KEYS

    root = {k: item[k] for k in reserved if k in item}
    setup_payload = {
        k: v for k, v in item.items() if k not in reserved and k != "setups"
    }

    setup_blob: dict[str, Any] = {"name": "DEFAULT", **setup_payload}
    if "enabled" not in setup_blob:
        setup_blob["enabled"] = item.get("enabled", True)

    root["setups"] = [setup_blob]
    return root


def _migrate_defaults_table(
    dynamodb, *, dry_run: bool
) -> tuple[int, int]:
    table = dynamodb.Table(DEFAULT_TABLE)
    scanned = 0
    updated = 0

    scan_kwargs: dict[str, Any] = {}
    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            scanned += 1
            if not _needs_setup_migration(item):
                continue
            new_item = _migrate_config_item(item, override_row=False)
            idx = new_item.get("index_name", "?")
            print(f"[defaults] migrate index_name={idx!r} dry_run={dry_run}")
            if not dry_run:
                table.put_item(Item=new_item)
            updated += 1

        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        scan_kwargs["ExclusiveStartKey"] = lek

    return scanned, updated


def _migrate_overrides_table(
    dynamodb, *, dry_run: bool
) -> tuple[int, int]:
    table = dynamodb.Table(OVERRIDE_TABLE)
    scanned = 0
    updated = 0

    scan_kwargs: dict[str, Any] = {}
    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            scanned += 1
            if not _needs_setup_migration(item):
                continue
            new_item = _migrate_config_item(item, override_row=True)
            cid = new_item.get("client_id", "?")
            idx = new_item.get("index_name", "?")
            print(f"[overrides] migrate client_id={cid!r} index_name={idx!r} dry_run={dry_run}")
            if not dry_run:
                table.put_item(Item=new_item)
            updated += 1

        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        scan_kwargs["ExclusiveStartKey"] = lek

    return scanned, updated


def _migrate_trades_table(
    dynamodb, *, dry_run: bool, client_id: str | None
) -> tuple[int, int]:
    table = dynamodb.Table(TRADES_TABLE)
    scanned = 0
    updated = 0

    scan_kwargs: dict[str, Any] = {}
    if client_id:
        scan_kwargs["FilterExpression"] = Attr("client_id").eq(client_id)

    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp.get("Items", []):
            scanned += 1
            if item.get("setup_name"):
                continue

            cid = item.get("client_id")
            tid = item.get("trade_id")
            if cid is None or tid is None:
                print(f"[trades] skip row missing keys: {item!r}", file=sys.stderr)
                continue

            print(f"[trades] set setup_name=DEFAULT client_id={cid!r} trade_id={tid!r} dry_run={dry_run}")
            if not dry_run:
                try:
                    table.update_item(
                        Key={"client_id": cid, "trade_id": tid},
                        UpdateExpression="SET setup_name = if_not_exists(setup_name, :d)",
                        ExpressionAttributeValues={":d": "DEFAULT"},
                    )
                except ClientError as e:
                    print(f"[trades] ERROR {cid}/{tid}: {e}", file=sys.stderr)
                    raise
            updated += 1

        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        scan_kwargs["ExclusiveStartKey"] = lek

    return scanned, updated


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Tip: point boto3 at Hong Kong with AWS_REGION=ap-east-1 before running.",
    )
    p.add_argument("--dry-run", action="store_true", help="Log actions only")
    p.add_argument("--defaults", action="store_true", help="Migrate DefaultAlgoConfig")
    p.add_argument("--overrides", action="store_true", help="Migrate ClientAlgoConfig")
    p.add_argument("--trades", action="store_true", help="Backfill ClientTrades.setup_name")
    p.add_argument(
        "--client-id",
        default=None,
        help="Limit trades scan to this client_id (recommended)",
    )
    args = p.parse_args()

    if not (args.defaults or args.overrides or args.trades):
        args.defaults = args.overrides = args.trades = True

    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

    trades_client = args.client_id or CLIENT_ID
    if args.trades and not trades_client:
        print(
            "Warning: --trades without --client-id and no CLIENT_ID in env will scan the full table.",
            file=sys.stderr,
        )

    if args.defaults:
        s, u = _migrate_defaults_table(dynamodb, dry_run=args.dry_run)
        print(f"[defaults] scanned={s} migrated={u}")

    if args.overrides:
        s, u = _migrate_overrides_table(dynamodb, dry_run=args.dry_run)
        print(f"[overrides] scanned={s} migrated={u}")

    if args.trades:
        s, u = _migrate_trades_table(
            dynamodb, dry_run=args.dry_run, client_id=trades_client
        )
        print(f"[trades] scanned={s} updated={u}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
