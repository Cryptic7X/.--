"""
BBW Telegram Alert System - 100% Range Entry Alerts  
Sends alerts when BBW first enters the 100% range (30M timeframe)
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
        # TradingView 30M chart
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=30"
        
        # CoinGlass liquidation heatmap
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        
        return tv_link, cg_link

    def send_bbw_batch_alert(self, signals: List[Dict]) -> bool:
        """Send consolidated BBW 100% range entry alert"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            # Build message header
            current_time = datetime.now().strftime('%H:%M:%S IST')
            message = f"""🔵 **BBW 30M - 100% RANGE ALERTS**

📊 **{len(signals)} FIRST-TIME RANGE ENTRIES DETECTED**

🕐 **{current_time}**

⏰ **Timeframe: 30M Candles**
🎯 **Alert Type: 100% Range Entry**

"""

            # Add range entry signals
            for i, signal in enumerate(signals, 1):
                symbol = signal['symbol']
                coin_data = signal['coin_data']
                price = self.format_price(coin_data['current_price'])
                change_24h = coin_data['price_change_percentage_24h']
                bbw_value = signal['bbw_value']
                contraction_line = signal['contraction_line']
                expansion_line = signal['expansion_line']
                range_pct = signal.get('range_percentage', 0)

                # Create chart links
                tv_link, cg_link = self.create_chart_links(symbol)

                message += f"""{i}. **{symbol}** | {price} ({change_24h:+.1f}% 24h)
📊 BBW: {bbw_value:.2f}
📉 Range: {contraction_line:.2f} - {expansion_line:.2f}
🎯 Position: {range_pct:.1f}% in range

📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Add summary
            message += f"""📊 **BBW 100% RANGE SUMMARY**

• Total Range Entries: {len(signals)}
• Timeframe: 30 Minutes
• Alert Logic: First entry into 100% range only
• Deduplication: Active (no repeats until exit + re-entry)

🎯 **Range represents full BBW volatility cycle from lowest contraction to highest expansion**"""

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
            
            print(f"📱 BBW 100% Range alert sent: {len(signals)} entries")
            return True

        except Exception as e:
            print(f"❌ BBW alert failed: {e}")
            return False
