"""
BBW Telegram Alert System - EXACT Format Match with 20H Reminders
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

    def create_chart_links(self, symbol: str) -> tuple:
        """Create TradingView and CoinGlass links"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=120"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link

    def send_bbw_batch_alert(self, signals: List[Dict]) -> bool:
        """Send BBW alerts in EXACT specified format"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            # Build message header - EXACT format match
            current_time = datetime.now().strftime('%H:%M:%S IST')
            message = f"""📊 BBW 2H SQUEEZE SIGNALS DETECTED
🕐 {current_time}
⏰ Timeframe: 2H Candles

"""

            # Add squeeze signals - EXACT format match
            for i, signal in enumerate(signals, 1):
                symbol = signal['symbol']
                coin_data = signal['coin_data']
                price = self.format_price(coin_data['current_price'])
                change_24h = coin_data['price_change_percentage_24h']
                
                bbw_value = signal['bbw_value']
                contraction_line = signal['lowest_contraction']
                squeeze_threshold = signal['squeeze_threshold']
                
                # Determine if first entry or reminder
                status = signal.get('alert_type', 'FIRST ENTRY')

                # Create chart links
                tv_link, cg_link = self.create_chart_links(symbol)

                message += f"""{i}. 🔵 SQUEEZE: {symbol}
💰 {price} ({change_24h:+.1f}% 24h)
📊 BBW: {bbw_value:.2f}
📉 Squeeze Range: {contraction_line:.2f} - {squeeze_threshold:.2f}
🎯 Status: {status}
📈 [Chart →]({tv_link}) | 🔥  [Liq Heat →]({cg_link})

"""

            # Add summary - EXACT format match
            message += f"""📊 BBW SQUEEZE SUMMARY
• Total Squeezes: {len(signals)}
• Squeeze Logic: BBW enters 75% above contraction"""

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

    def send_reminder_alert(self, long_squeeze_coins: List[Dict]) -> bool:
        """Send 20-hour reminder alerts for extended squeezes"""
        if not self.bot_token or not self.chat_id or not long_squeeze_coins:
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            message = f"""📊 BBW 2H LONG SQUEEZE REMINDERS
🕐 {current_time}
⏰ Timeframe: 2H Candles

"""

            for i, signal in enumerate(long_squeeze_coins, 1):
                symbol = signal['symbol']
                hours_in_squeeze = signal.get('hours_in_squeeze', 20)
                
                message += f"""{i}. 🔵 LONG SQUEEZE: {symbol}
🎯 Status: Still in squeeze ({hours_in_squeeze}+ hours)
📊 {symbol} is still in a long squeeze

"""

            message += f"""📊 LONG SQUEEZE SUMMARY
• Total Long Squeezes: {len(long_squeeze_coins)}
• Alert Type: 20+ Hour Reminders
• Action: Monitor for breakout potential"""

            # Send reminder message
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': True
            }

            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            print(f"📱 BBW reminder alert sent: {len(long_squeeze_coins)} long squeezes")
            return True

        except Exception as e:
            print(f"❌ BBW reminder alert failed: {e}")
            return False
