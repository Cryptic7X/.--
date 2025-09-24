"""
BBW Telegram Alert System - First-Touch Squeeze Alerts
Sends alerts to BBW channel when BBW touches contraction line
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
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=15"
        
        # CoinGlass liquidation heatmap
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        
        return tv_link, cg_link
    
    def send_bbw_batch_alert(self, signals: List[Dict]) -> bool:
        """Send consolidated BBW squeeze alert"""
        if not self.bot_token or not self.chat_id or not signals:
            return False
        
        try:
            # Build message header
            current_time = datetime.now().strftime('%H:%M:%S IST')
            message = f"""🔵 **BBW 15M SQUEEZE ALERTS**

📊 **{len(signals)} FIRST-TOUCH SQUEEZES DETECTED**
🕐 **{current_time}**
⏰ **Timeframe: 15M Candles**

"""
            
            # Add squeeze signals
            for i, signal in enumerate(signals, 1):
                symbol = signal['symbol']
                coin_data = signal['coin_data']
                price = self.format_price(coin_data['current_price'])
                change_24h = coin_data['price_change_percentage_24h']
                bbw_value = signal['bbw_value']
                contraction_line = signal['contraction_line']
                
                # Create chart links
                tv_link, cg_link = self.create_chart_links(symbol)
                
                message += f"""{i}. **{symbol}** | {price} ({change_24h:+.1f}% 24h)
📊 BBW: {bbw_value}
📉 L-C: {contraction_line}
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""
            
            # Add summary
            message += f"""📊 **BBW SUMMARY**
• Total Squeezes: {len(signals)}

🎯 Manual direction analysis required"""
            
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
