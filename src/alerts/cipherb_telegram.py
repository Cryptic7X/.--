"""
CipherB Telegram Alert System - Enhanced Format with Context
Sends consolidated alerts to CipherB channel
"""

import os
import requests
from datetime import datetime
from typing import List, Dict

class CipherBTelegramSender:
    def __init__(self, config: Dict):
        self.config = config
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('CIPHERB_TELEGRAM_CHAT_ID')
        
    def format_price(self, price: float) -> str:
        """Format price for display"""
        if price < 0.001:
            return f"${price:.8f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:.2f}"
    
    def format_large_number(self, num: float) -> str:
        """Format large numbers"""
        if num >= 1_000_000_000:
            return f"${num/1_000_000_000:.1f}B"
        elif num >= 1_000_000:
            return f"${num/1_000_000:.0f}M"
        else:
            return f"${num/1_000:.0f}K"
    
    def create_chart_links(self, symbol: str) -> tuple:
        """Create TradingView and CoinGlass links"""
        # TradingView 2H chart
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=120"
        
        # CoinGlass liquidation heatmap
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        
        return tv_link, cg_link
    
    def send_cipherb_batch_alert(self, signals: List[Dict]) -> bool:
        """Send consolidated CipherB alert"""
        if not self.bot_token or not self.chat_id or not signals:
            return False
        
        try:
            # Group signals by type
            buy_signals = [s for s in signals if s['signal_type'] == 'BUY']
            sell_signals = [s for s in signals if s['signal_type'] == 'SELL']
            
            # Build message header
            current_time = datetime.now().strftime('%H:%M:%S IST')
            message = f"""🎯 **CIPHERB 2H SIGNALS**

📊 **{len(signals)} PRECISE SIGNALS DETECTED**
🕐 **{current_time}**
⏰ **Timeframe: 2H Candles**

"""
            
            # Add BUY signals
            if buy_signals:
                message += "🟢 **BUY SIGNALS:**\n"
                for i, signal in enumerate(buy_signals, 1):
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    
                    # Create chart links
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""{i}. **{symbol}** | {price} | {change_24h:+.1f}%
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""
            
            # Add SELL signals
            if sell_signals:
                message += "🔴 **SELL SIGNALS:**\n"
                for i, signal in enumerate(sell_signals, 1):
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    
                    # Create chart links
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""{i}. **{symbol}** | {price} | {change_24h:+.1f}%
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""
            
            # Add summary
            message += f"""📊 **CIPHERB SIGNAL SUMMARY**
• Total Signals: {len(signals)}
• Buy Signals: {len(buy_signals)}
• Sell Signals: {len(sell_signals)}"""
            
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
            
            print(f"📱 CipherB alert sent: {len(signals)} signals")
            return True
            
        except Exception as e:
            print(f"❌ CipherB alert failed: {e}")
            return False
