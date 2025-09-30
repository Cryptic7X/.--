"""
EMA Telegram Alert System - 12/21 EMA CROSSOVER ANALYSIS
2H Timeframe - Simple Crossover Only
"""
import os
import requests
from datetime import datetime
from typing import List, Dict

class EMATelegramSender:
    def __init__(self, config: Dict):
        self.config = config
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('SMA_TELEGRAM_CHAT_ID')

    def format_price(self, price: float) -> str:
        if price < 0.001:
            return f"${price:.8f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:.2f}"

    def format_large_number(self, num: float) -> str:
        if num >= 1_000_000_000:
            return f"${num/1_000_000_000:.0f}B"
        elif num >= 1_000_000:
            return f"${num/1_000_000:.0f}M"
        else:
            return f"${num/1_000:.0f}K"

    def create_chart_links(self, symbol: str, timeframe_minutes: int = 120) -> tuple:
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe_minutes}"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link

    def send_ema_alerts(self, signals: List[Dict], timeframe_minutes: int = 120) -> bool:
        """12/21 EMA CROSSOVER ALERTS - 2H TIMEFRAME"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            message = f"""📊 EMA 2H SIGNALS DETECTED

🕐 {current_time}
⏰ Timeframe: 2H Candles
🔄 12/21 EMA CROSSOVER SIGNALS:"""

            # Group signals by type
            golden_signals = [s for s in signals if s.get('crossover_type') == 'golden_cross']
            death_signals = [s for s in signals if s.get('crossover_type') == 'death_cross']

            # Golden Cross signals
            if golden_signals:
                message += "\n🟡GOLDEN CROSS (12>21): "
                
                for i, signal in enumerate(golden_signals, 1):
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    market_cap = self.format_large_number(coin_data.get('market_cap', 0))
                    volume = self.format_large_number(coin_data.get('total_volume', 0))
                    
                    tv_link, cg_link = self.create_chart_links(symbol, timeframe_minutes)
                    
                    message += f"""

{i}. {symbol} | 💰 {price} | ({change_24h:+.1f}%)
Cap: {market_cap} | Vol: {volume}
📈[Chart →]({tv_link}) |🔥 [Liq Heat →]({cg_link})"""

            # Death Cross signals
            if death_signals:
                message += "\n🔴DEATH CROSS (12<21): "
                
                for i, signal in enumerate(death_signals, 1):
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    market_cap = self.format_large_number(coin_data.get('market_cap', 0))
                    volume = self.format_large_number(coin_data.get('total_volume', 0))
                    
                    tv_link, cg_link = self.create_chart_links(symbol, timeframe_minutes)
                    
                    message += f"""

{i}. {symbol} | 💰 {price} | ({change_24h:+.1f}%)
Cap: {market_cap} | Vol: {volume}
📈[Chart →]({tv_link}) |🔥 [Liq Heat →]({cg_link})"""

            # Summary
            total_crossovers = len(golden_signals) + len(death_signals)
            golden_count = len(golden_signals)
            death_count = len(death_signals)

            message += f"""

📊 EMA SUMMARY
• Total Crossovers: {total_crossovers} (🟡 {golden_count} Golden, 🔴 {death_count} Death)
🎯 EMA Strategy: 12/21 crossover system
⚡ 2H timeframe for strong signals
🚫 6H cooldown prevents spam"""

            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }

            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            return True

        except Exception as e:
            print(f"❌ Telegram send error: {e}")
            return False
