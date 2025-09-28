#!/usr/bin/env python3
"""
SimpleDataFetcher 2.1
• Reads thresholds from config/config.yaml
• Uses server-side volume filter (≥ $10 M) to cut credits
• Pages in blocks of 2 000 to avoid incomplete results
• Keeps client-side market-cap filtering for accuracy
"""

import os, json, requests, yaml
from datetime import datetime
from typing import List, Tuple

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
CACHE_DIR   = "cache"
CMC_URL     = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
PAGE_LIMIT  = 2_000                    # reliable page size
VOLUME_MIN  = 10_000_000               # $10 M server-side filter
TIMEOUT     = 30                       # seconds

class SimpleDataFetcher:
    def __init__(self) -> None:
        self.api_key = os.getenv("COINMARKETCAP_API_KEY")
        if not self.api_key:
            raise SystemExit("❌ COINMARKETCAP_API_KEY not set")
        self.config        = self._load_config()
        self.blocked_coins = self._load_blocked_coins()
        print(f"🛑 Blocked coins loaded: {len(self.blocked_coins)}")

    # ---------- helpers ---------------------------------------------------
    @staticmethod
    def _load_config() -> dict:
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f)

    @staticmethod
    def _load_blocked_coins() -> set:
        path = os.path.join(os.path.dirname(__file__), "..", "config", "blocked_coins.txt")
        blocked = set()
        with open(path, "r") as f:
            for line in f:
                coin = line.strip().upper()
                if coin and not coin.startswith("#"):
                    blocked.add(coin)
        return blocked

    # ---------- core ------------------------------------------------------
    def fetch_all_pages(self) -> List[dict]:
        headers = {"X-CMC_PRO_API_KEY": self.api_key}
        all_coins, start = [], 1

        print("🚀 Fetching coins from CoinMarketCap …")
        while True:
            params = {
                "start"          : start,
                "limit"          : PAGE_LIMIT,
                "sort"           : "market_cap",
                "convert"        : "USD",
                "volume_24h_min" : VOLUME_MIN      # ← server-side filter
            }
            r = requests.get(CMC_URL, headers=headers, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json().get("data", [])
            if not data:
                break

            all_coins.extend(data)
            if len(data) < PAGE_LIMIT:
                break                              # last page received
            start += PAGE_LIMIT                   # next page

        print(f"📊 Total coins fetched: {len(all_coins)}")
        return all_coins

    # ---------- client-side filters ---------------------------------------
    def partition(self, coins: List[dict]) -> Tuple[List[dict], List[dict]]:
        f_cfg   = self.config["market_filters"]
        bbw_cfg = f_cfg["cipherb_bbw"]
        ema_cfg = f_cfg["ema"]

        cipherb_coins = []
        ema_coins     = []

        for coin in coins:
            sym   = coin["symbol"].upper()
            if sym in self.blocked_coins:
                continue

            quote       = coin["quote"]["USD"]
            mcap        = quote["market_cap"]     or 0
            volume      = quote["volume_24h"]     or 0

            # CipherB / BBW
            if mcap >= bbw_cfg["min_market_cap"] and volume >= bbw_cfg["min_volume_24h"]:
                cipherb_coins.append(self._shrink(coin))

            # EMA
            if (ema_cfg["min_market_cap"] <= mcap <= ema_cfg["max_market_cap"]
                    and volume >= ema_cfg["min_volume_24h"]):
                ema_coins.append(self._shrink(coin))

        print(f"📊 CipherB/BBW coins: {len(cipherb_coins)}")
        print(f"📊 EMA coins       : {len(ema_coins)}")
        return cipherb_coins, ema_coins

    @staticmethod
    def _shrink(c: dict) -> dict:
        """Keep only the fields the analyzers need."""
        q = c["quote"]["USD"]
        return {
            "symbol"        : c["symbol"],
            "name"          : c["name"],
            "market_cap"    : q["market_cap"],
            "current_price" : q["price"],
            "total_volume"  : q["volume_24h"],
            "last_updated"  : c["last_updated"]
        }

    # ---------- cache ------------------------------------------------------
    def save(self, cipherb: List[dict], ema: List[dict]) -> None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        now = datetime.utcnow().isoformat()

        json.dump({"timestamp": now, "total": len(cipherb), "coins": cipherb},
                  open(os.path.join(CACHE_DIR, "cipherb_dataset.json"), "w"))
        json.dump({"timestamp": now, "total": len(ema), "coins": ema},
                  open(os.path.join(CACHE_DIR, "ema_dataset.json"), "w"))
        print("💾 Datasets saved to cache/")

# -------------------------------------------------------------------------
if __name__ == "__main__":
    fetcher            = SimpleDataFetcher()
    raw_coins          = fetcher.fetch_all_pages()
    cipherb, ema       = fetcher.partition(raw_coins)
    fetcher.save(cipherb, ema)
    print("✅ Daily data collection complete")
else:
    print("❌ No coins fetched")

if __name__ == "__main__":
    main()
