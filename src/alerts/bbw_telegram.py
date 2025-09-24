"""
BBW Telegram Alert System - Professional MT Alert Format
Matches CipherB and SMA alert formatting standards
"""
import os
import requests
from datetime import datetime
from typing import List, Dict

class BBWTelegramSender:
    def __init__(self, config: Dict):
        self.config = config
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('BBW_TELEGRAM_CHAT_ID')

    def format_price(self, price: float) -> str:
        """Format price for display - Same as CipherB/SMA"""
        if price < 0.001:
            return f"${price:.8f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:.2f}"

    def format_large_number(self, num: float) -> str:
        """Format large numbers - Same as SMA"""
        if num >= 1_000_000_000:
            return f"${num/1_000_000_000:.1f}B"
        elif num >= 1_000_000:
            return f"${num/1_000_000:.0f}M"
        else:
            return f"${num/1_000:.0f}K"

    def create_chart_links(self, symbol: str) -> tuple:
        """Create TradingView and CoinGlass links - Same as CipherB/SMA"""
        # TradingView 2H chart (same as other systems)
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=120"
        
        # CoinGlass liquidation heatmap
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        
        return tv_link, cg_link

    def send_bbw_alerts(self, signals: List[Dict]) -> bool:
        """Send BBW alerts with professional MT format - Matches CipherB/SMA"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            # Build message header (same format as CipherB/SMA)
            current_time = datetime.now().strftime('%H:%M:%S IST')
            message = f"""🔵 **BBW 2H SIGNALS**

📊 **{len(signals)} SQUEEZE SIGNALS DETECTED**

🕐 **{current_time}**

⏰ **Timeframe: 2H Candles**

"""

            # Add squeeze signals (same detailed format as SMA)
            message += "🔵 **SQUEEZE SIGNALS:**\n"
            
            for i, signal in enumerate(signals, 1):
                symbol = signal['symbol']
                coin_data = signal['coin_data']
                price = self.format_price(coin_data['current_price'])
                change_24h = coin_data['price_change_percentage_24h']
                market_cap = self.format_large_number(coin_data['market_cap'])
                volume = self.format_large_number(coin_data['total_volume'])
                
                bbw_value = signal['bbw_value']
                contraction_line = signal['lowest_contraction']
                squeeze_threshold = signal['squeeze_threshold']

                # Create chart links
                tv_link, cg_link = self.create_chart_links(symbol)

                message += f"""🔵 **SQUEEZE: {symbol}**

💰 {price} ({change_24h:+.1f}% 24h)
Cap: {market_cap} | Vol: {volume}

📊 BBW: {bbw_value:.2f}
📉 Squeeze Range: {contraction_line:.2f} - {squeeze_threshold:.2f}
🎯 Status: FIRST ENTRY

📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Add summary (same format as CipherB/SMA)
            message += f"""📊 **BBW SQUEEZE SUMMARY**

• Total Squeezes: {len(signals)}
• Squeeze Logic: BBW enters 75% above contraction
• Deduplication: Once per 24H per symbol
🎯 **Volatility contraction = Breakout potential**"""

            # Send message
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }

            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            print(f"📱 BBW alert sent: {len(signals)} squeezes")
            return True

        except Exception as e:
            print(f"❌ BBW alert failed: {e}")
            return False
