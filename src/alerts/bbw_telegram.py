"""
SIMPLE BBW Telegram - No Complexity
"""
import os
import requests
from datetime import datetime

class BBWTelegramSender:
    def __init__(self, config):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('BBW_TELEGRAM_CHAT_ID')

    def send_bbw_alerts(self, signals):
        """Send BBW alerts - SIMPLE"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            
            # Build simple message
            message = f"🔵 **BBW SQUEEZE ALERTS** - {current_time}\n\n"
            
            for i, signal in enumerate(signals, 1):
                symbol = signal['symbol']
                price = signal['coin_data']['current_price']
                bbw = signal['bbw_value']
                contraction = signal['lowest_contraction']
                threshold = signal['squeeze_threshold']
                
                # Format price
                if price < 0.001:
                    price_str = f"${price:.6f}"
                elif price < 1:
                    price_str = f"${price:.4f}"
                else:
                    price_str = f"${price:.2f}"
                
                message += f"{i}. **{symbol}** | {price_str}\n"
                message += f"🔵 BBW: {bbw:.2f}\n"
                message += f"📉 Squeeze: {contraction:.2f} - {threshold:.2f}\n\n"
            
            message += f"📊 **Total:** {len(signals)} squeezes detected"
            message += f"\n⚡ **Timeframe:** 2H"
            message += f"\n🎯 **Logic:** BBW enters 75% above contraction"
            
            # Send to Telegram
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            
            print(f"📱 BBW alert sent: {len(signals)} squeezes")
            return True
            
        except Exception as e:
            print(f"❌ Telegram failed: {e}")
            return False
