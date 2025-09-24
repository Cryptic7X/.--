"""
BBW Telegram Alert System - FIXED (No 400 errors)
Simple, robust format that always works
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
        """Format price safely"""
        try:
            if price < 0.001:
                return f"${price:.6f}"
            elif price < 1:
                return f"${price:.4f}"
            else:
                return f"${price:.2f}"
        except:
            return "$0.00"

    def format_number(self, num: float) -> str:
        """Format large numbers safely"""
        try:
            if num >= 1_000_000_000:
                return f"${num/1_000_000_000:.1f}B"
            elif num >= 1_000_000:
                return f"${num/1_000_000:.0f}M"
            else:
                return f"${num/1_000:.0f}K"
        except:
            return "$0"

    def send_bbw_alerts(self, signals: List[Dict]) -> bool:
        """Send BBW alerts - SIMPLE and RELIABLE"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            
            # Keep it SIMPLE - no complex markdown
            message = f"🔵 BBW SQUEEZE ALERTS - {current_time}\n\n"
            message += f"📊 {len(signals)} SQUEEZES DETECTED (2H)\n\n"
            
            # Add each signal (keep simple)
            for i, signal in enumerate(signals, 1):
                if i > 10:  # Limit to prevent message being too long
                    message += f"... and {len(signals) - 10} more\n"
                    break
                    
                symbol = signal['symbol']
                coin_data = signal['coin_data']
                
                # Safe formatting
                price = self.format_price(coin_data.get('current_price', 0))
                change_24h = coin_data.get('price_change_percentage_24h', 0)
                
                bbw_value = signal.get('bbw_value', 0)
                contraction = signal.get('lowest_contraction', 0)
                threshold = signal.get('squeeze_threshold', 0)
                
                message += f"{i}. {symbol} | {price} ({change_24h:+.1f}%)\n"
                message += f"   BBW: {bbw_value:.2f} | Range: {contraction:.2f}-{threshold:.2f}\n\n"
            
            message += f"🎯 Squeeze Logic: BBW enters 75% above contraction\n"
            message += f"⏰ Deduplication: Once per 24H per symbol"
            
            # Send with basic formatting only
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message[:4000],  # Ensure under Telegram limit
                'disable_web_page_preview': True  # Prevent any link issues
            }

            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                print(f"📱 BBW alert sent: {len(signals)} squeezes")
                return True
            else:
                print(f"❌ Telegram error {response.status_code}: {response.text}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ Telegram request failed: {e}")
            return False
        except Exception as e:
            print(f"❌ BBW alert error: {e}")
            return False
