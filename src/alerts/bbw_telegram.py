"""
BBW Telegram Alert System - FIXED (Shorter Messages)
Sends alerts when BBW first enters squeeze range (2H timeframe)
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
        """Format price for display"""
        if price < 0.001:
            return f"${price:.8f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:.2f}"

    def create_chart_links(self, symbol: str) -> str:
        """Create TradingView link only"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        return f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=120"

    def send_bbw_batch_alert(self, signals: List[Dict]) -> bool:
        """Send consolidated BBW squeeze entry alert - SHORTER VERSION"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            # Limit to 10 signals max to avoid message size limit
            display_signals = signals[:10]
            
            # Build shorter message
            current_time = datetime.now().strftime('%H:%M IST')
            message = f"🔵 **BBW 2H SQUEEZE** - {current_time}\n\n"
            message += f"📊 **{len(signals)} entries detected**\n\n"

            # Add squeeze entries (shortened format)
            for i, signal in enumerate(display_signals, 1):
                symbol = signal['symbol']
                coin_data = signal['coin_data']
                price = self.format_price(coin_data['current_price'])
                change_24h = coin_data['price_change_percentage_24h']
                bbw_value = signal['bbw_value']
                contraction = signal['contraction_line']
                threshold_75 = signal.get('threshold_75', 0)

                # Create chart link
                tv_link = self.create_chart_links(symbol)

                message += f"{i}. **{symbol}** | {price} ({change_24h:+.1f}%)\n"
                message += f"BBW: {bbw_value:.1f} | Range: {contraction:.1f}-{threshold_75:.1f}\n"
                message += f"[Chart]({tv_link})\n\n"

            # Add summary if more than 10 signals
            if len(signals) > 10:
                message += f"... and {len(signals) - 10} more signals\n\n"

            message += f"🎯 **Squeeze = Low volatility → Breakout potential**"

            # Send message (NO parse_mode to avoid Markdown errors)
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'disable_web_page_preview': True
            }

            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            print(f"📱 BBW squeeze alert sent: {len(signals)} entries")
            return True

        except Exception as e:
            print(f"❌ BBW alert failed: {e}")
            return False
