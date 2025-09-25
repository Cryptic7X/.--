"""
BBW Telegram Alert System - EXACT Format Match
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
        """Send BBW alerts in EXACT format you specified"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            # Separate first entry and reminder alerts
            first_entry_signals = [s for s in signals if s.get('alert_type') == 'FIRST ENTRY']
            reminder_signals = [s for s in signals if s.get('alert_type') == 'EXTENDED SQUEEZE']
            
            # Build message header - EXACT format match
            current_time = datetime.now().strftime('%H:%M:%S IST')
            
            if first_entry_signals:
                message = f"""📊 BBW 2H SQUEEZE SIGNALS DETECTED
🕐 {current_time}
⏰ Timeframe: 2H Candles

"""
                
                # Add first entry signals - EXACT format
                for i, signal in enumerate(first_entry_signals, 1):
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    
                    bbw_value = signal['bbw_value']
                    contraction_line = signal['lowest_contraction']
                    range_top = signal['range_top']

                    # Create chart links
                    tv_link, cg_link = self.create_chart_links(symbol)

                    message += f"""{i}. 🔵 SQUEEZE: {symbol}
💰 {price} ({change_24h:+.1f}% 24h)
📊 BBW: {bbw_value:.2f}
📉 Squeeze Range: {contraction_line:.2f} - {range_top:.2f}
🎯 Status: FIRST ENTRY
📈 [Chart →]({tv_link}) | 🔥  [Liq Heat →]({cg_link})

"""

                # Add summary - EXACT format
                message += f"""📊 BBW SQUEEZE SUMMARY
• Total Squeezes: {len(first_entry_signals)}
• Squeeze Logic: BBW enters 100% above contraction

"""

            # Add reminder alerts if any
            if reminder_signals:
                if first_entry_signals:
                    message += "\n"
                
                message += f"""🔔 EXTENDED SQUEEZE REMINDERS

"""
                
                for signal in reminder_signals:
                    symbol = signal['symbol']
                    message += f"• {symbol} is still in a long squeeze (20+ hours)\n"

            # Send message
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message.strip(),
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }

            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            print(f"📱 BBW alert sent: {len(first_entry_signals)} squeezes, {len(reminder_signals)} reminders")
            return True

        except Exception as e:
            print(f"❌ BBW alert failed: {e}")
            return False
