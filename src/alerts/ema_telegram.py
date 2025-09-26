"""
EMA Telegram Alert System - CLEAN VERSION
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
            return f"${num/1_000_000_000:.1f}B"
        elif num >= 1_000_000:
            return f"${num/1_000_000:.0f}M"
        else:
            return f"${num/1_000:.0f}K"

    def create_chart_links(self, symbol: str, timeframe_minutes: int = 240) -> tuple:
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe_minutes}"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link

    def determine_zone_info(self, ema21: float, ema50: float) -> Dict:
        if ema21 > ema50:
            return {
                'setup_type': "Bullish Setup",
                'zone_type': "Support Zone",
                'zone_range': f"[{self.format_price(ema50)} - {self.format_price(ema21)}]"
            }
        else:
            return {
                'setup_type': "Bearish Setup",
                'zone_type': "Resistance Zone",
                'zone_range': f"[{self.format_price(ema21)} - {self.format_price(ema50)}]"
            }

    def send_ema_alerts(self, signals: List[Dict], timeframe_minutes: int = 30) -> bool:
    if not self.bot_token or not self.chat_id or not signals:
        return False
    
    try:
        current_time = datetime.now().strftime('%H:%M:%S IST')
        total_alerts = len(signals)
        tf_display = "30M Candles" if timeframe_minutes == 30 else f"{timeframe_minutes}M Candles"
        
        message = f"""🟡 **EMA 30M SIGNALS**

📊 **{total_alerts} CROSSOVER SIGNALS DETECTED**

🕐 **{current_time}**

⏰ **Timeframe: {tf_display}**

"""
        
        # ONLY CROSSOVER SIGNALS (zone logic completely removed)
        message += "🔄 **CROSSOVER SIGNALS:**\n"
        
        for signal in signals:
            symbol = signal['symbol']
            coin_data = signal['coin_data']
            crossover_type = signal['crossover_type']
            price = self.format_price(coin_data['current_price'])
            change_24h = coin_data.get('price_change_percentage_24h', 0)
            market_cap = self.format_large_number(coin_data.get('market_cap', 0))
            volume = self.format_large_number(coin_data.get('total_volume', 0))
            
            signal_emoji = "🟡" if crossover_type == 'golden_cross' else "🔴"
            signal_name = "GOLDEN CROSS" if crossover_type == 'golden_cross' else "DEATH CROSS"
            
            tv_link, cg_link = self.create_chart_links(symbol, timeframe_minutes)
            
            message += f"""{signal_emoji} **{signal_name}: {symbol}**

💰 {price} ({change_24h:+.1f}% 24h)

Cap: {market_cap} | Vol: {volume}

📊 21 EMA: {self.format_price(signal['ema21'])}

📊 50 EMA: {self.format_price(signal['ema50'])}

📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""
        
        golden_count = len([s for s in signals if s.get('crossover_type') == 'golden_cross'])
        death_count = len([s for s in signals if s.get('crossover_type') == 'death_cross'])
        
        message += f"""📊 **EMA SUMMARY**

• Total Crossovers: {len(signals)} (🟡 {golden_count} Golden, 🔴 {death_count} Death)

🎯 Small-cap focus: $10M-$500M market cap

⚡ 30-minute timeframe for responsive signals"""

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
            return False
