#!/usr/bin/env python3

import argparse
import sys
from typing import Any

import requests


def str_to_bool_flag(value: str) -> int:
    v = value.strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return 1
    if v in {"0", "false", "no", "off"}:
        return 0
    raise argparse.ArgumentTypeError(f"invalid boolean flag: {value}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Configure a SparkMiner device via its AP portal"
    )

    p.add_argument("--host", default="192.168.4.1", help="SparkMiner AP host or IP")
    p.add_argument("--ssid", required=True, help="Wi-Fi SSID")
    p.add_argument("--wifi-password", required=True, help="Wi-Fi password")

    p.add_argument("--wallet", required=True, help="Primary wallet address")
    p.add_argument("--worker", default="SparkMiner", help="Worker name")

    p.add_argument("--pool-url", default="public-pool.io", help="Primary pool host")
    p.add_argument("--pool-port", type=int, default=21496, help="Primary pool port")
    p.add_argument("--pool-pass", default="x", help="Primary pool password")

    p.add_argument("--backup-pool-url", default="pool.nerdminers.org", help="Backup pool host")
    p.add_argument("--backup-pool-port", type=int, default=3333, help="Backup pool port")
    p.add_argument("--backup-wallet", default="", help="Backup wallet address")
    p.add_argument("--backup-pool-pass", default="x", help="Backup pool password")

    p.add_argument("--brightness", type=int, default=100, help="Brightness 0-100")
    p.add_argument("--screen-timeout", type=int, default=0, help="Screen timeout in seconds")
    p.add_argument("--diff", type=float, default=0.0014, help="Target difficulty")
    p.add_argument("--rotation", type=int, default=0, choices=[0, 1, 2, 3], help="Screen rotation")
    p.add_argument("--tz", type=int, default=0, help="Timezone offset or portal tz value")

    p.add_argument("--invert", type=str_to_bool_flag, default=1, help="Invert colors: 0/1")
    p.add_argument("--stats-en", type=str_to_bool_flag, default=1, help="Enable stats: 0/1")
    p.add_argument("--https-stats", type=str_to_bool_flag, default=0, help="Enable HTTPS stats: 0/1")

    p.add_argument("--stats-api", default="", help="Custom stats API URL")
    p.add_argument("--stats-proxy", default="", help="Stats proxy URL")

    p.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    p.add_argument(
        "--insecure",
        action="store_true",
        help="Skip TLS verification if using HTTPS host",
    )

    return p


def build_form(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "s": args.ssid,
        "p": args.wifi_password,
        "wallet": args.wallet,
        "worker": args.worker,
        "pool_url": args.pool_url,
        "pool_port": str(args.pool_port),
        "pool_pass": args.pool_pass,
        "bk_pool_url": args.backup_pool_url,
        "bk_pool_port": str(args.backup_pool_port),
        "bk_wallet": args.backup_wallet,
        "bk_pool_pass": args.backup_pool_pass,
        "bright": str(args.brightness),
        "scrn_to": str(args.screen_timeout),
        "diff": f"{args.diff:.6f}",
        "rotation": str(args.rotation),
        "tz": str(args.tz),
        "invert": str(args.invert),
        "stats_en": str(args.stats_en),
        "stats_api": args.stats_api,
        "stats_proxy": args.stats_proxy,
        "https_stats": str(args.https_stats),
    }


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    scheme = "https" if str(args.host).startswith("https://") else "http"
    if args.host.startswith("http://") or args.host.startswith("https://"):
        url = f"{args.host.rstrip('/')}/wifisave"
        origin = args.host.rstrip("/")
        referer = f"{origin}/wifi?"
    else:
        url = f"{scheme}://{args.host}/wifisave"
        origin = f"{scheme}://{args.host}"
        referer = f"{origin}/wifi?"

    form = build_form(args)

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": origin,
        "Referer": referer,
        "User-Agent": "sparkminer-config/1.0",
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            data=form,
            timeout=args.timeout,
            verify=not args.insecure,
        )
    except requests.RequestException as exc:
        print(f"request failed: {exc}", file=sys.stderr)
        return 1

    print(f"status: {response.status_code}")
    # print(response.text)

    if response.ok:
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())