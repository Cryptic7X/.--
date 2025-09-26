"""
CipherB Multi-Timeframe Telegram Alert System
Supports: 2H SIGNAL, 2H REPEATED, 2H+8H CONFIRMED alerts
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
        try:
            if price < 0.001:
                return f"${price:.8f}"
            elif price < 1:
                return f"${price:.4f}"
            else:
                return f"${price:.2f}"
        except:
            return "$0.00"

    def format_large_number(self, num: float) -> str:
        """Format large numbers"""
        try:
            if num >= 1_000_000_000:
                return f"${num/1_000_000_000:.1f}B"
            elif num >= 1_000_000:
                return f"${num/1_000_000:.0f}M"
            else:
                return f"${num/1_000:.0f}K"
        except:
            return "$0"

    def create_chart_links(self, symbol: str) -> tuple:
        """Create TradingView and CoinGlass links for 2H timeframe"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=120"  # 2H = 120 minutes
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link

    def send_cipherb_multi_alerts(self, alerts: List[Dict]) -> bool:
        """Send multi-timeframe CipherB alerts"""
        if not self.bot_token or not self.chat_id or not alerts:
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            total_alerts = len(alerts)
            
            # Group alerts by type
            signal_2h = [a for a in alerts if a['message_type'] == '2H_SIGNAL']
            repeated_2h = [a for a in alerts if a['message_type'] == '2H_REPEATED']
            confirmed_2h8h = [a for a in alerts if a['message_type'] == '2H_8H_CONFIRMED']
            
            message = f"""🔵 **CIPHERB MULTI-TIMEFRAME SIGNALS**
📊 **{total_alerts} CIPHERB SIGNALS DETECTED**
🕐 **{current_time}**
⏰ **2H Primary + 8H Confirmation**

"""

            # 1. First-time 2H signals
            if signal_2h:
                message += f"🎯 **2H SIGNALS ({len(signal_2h)}):**\n"
                
                for alert in signal_2h:
                    symbol = alert['symbol']
                    signal_type = alert['alert_type']
                    coin_data = alert['coin_data']
                    signal_2h_data = alert['signal_2h']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    market_cap = self.format_large_number(coin_data.get('market_cap', 0))
                    volume = self.format_large_number(coin_data.get('total_volume', 0))
                    
                    signal_emoji = "🟢" if signal_type == 'BUY' else "🔴"
                    
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""{signal_emoji} **2H SIGNAL: {symbol} {signal_type}**
💰 {price} ({change_24h:+.1f}% 24h)
Cap: {market_cap} | Vol: {volume}
📊 WT1: {signal_2h_data['wt1']} | WT2: {signal_2h_data['wt2']}
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # 2. Repeated 2H signals (2nd time same direction)
            if repeated_2h:
                message += f"🔄 **2H REPEATED SIGNALS ({len(repeated_2h)}):**\n"
                
                for alert in repeated_2h:
                    symbol = alert['symbol']
                    signal_type = alert['alert_type']
                    coin_data = alert['coin_data']
                    signal_2h_data = alert['signal_2h']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    
                    signal_emoji = "🟡" if signal_type == 'BUY' else "🟠"
                    
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""{signal_emoji} **2H REPEATED: {symbol} {signal_type}** (2nd same-direction)
💰 {price} ({change_24h:+.1f}% 24h)
📊 WT1: {signal_2h_data['wt1']} | WT2: {signal_2h_data['wt2']}
🔍 Added to 8H monitoring list
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # 3. 8H confirmed signals
            if confirmed_2h8h:
                message += f"✅ **2H+8H CONFIRMED SIGNALS ({len(confirmed_2h8h)}):**\n"
                
                for alert in confirmed_2h8h:
                    symbol = alert['symbol']
                    signal_type = alert['alert_type']
                    coin_data = alert['coin_data']
                    signal_2h_data = alert['signal_2h']
                    signal_8h_data = alert['signal_8h']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    
                    signal_emoji = "✅" if signal_type == 'BUY' else "❌"
                    
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""{signal_emoji} **2H+8H CONFIRMED: {symbol} {signal_type}**
💰 {price} ({change_24h:+.1f}% 24h)
📊 2H WT1: {signal_2h_data['wt1']} | WT2: {signal_2h_data['wt2']}
📊 8H WT1: {signal_8h_data['wt1']} | WT2: {signal_8h_data['wt2']}
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Summary
            buy_signals = len([a for a in alerts if a['alert_type'] == 'BUY'])
            sell_signals = len([a for a in alerts if a['alert_type'] == 'SELL'])
            
            message += f"""📊 **CIPHERB SUMMARY**
• Total Alerts: {total_alerts}
• Buy Signals: {buy_signals} | Sell Signals: {sell_signals}
• 2H New: {len(signal_2h)} | 2H Repeated: {len(repeated_2h)} | 2H+8H: {len(confirmed_2h8h)}
🎯 Multi-timeframe confirmation active"""

            # Send to Telegram
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }

            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            print(f"📱 CipherB multi-timeframe alert sent: {total_alerts} signals")
            return True

        except Exception as e:
            print(f"❌ CipherB alert failed: {e}")
            return False

    def send_cipherb_batch_alert(self, signals: List[Dict]) -> bool:
        """Legacy method for backward compatibility"""
        # Convert legacy format to new format
        alerts = []
        for signal in signals:
            alert = {
                'symbol': signal['symbol'],
                'alert_type': signal['signal_type'],
                'message_type': '2H_SIGNAL',  # Default to 2H signal
                'signal_2h': signal,
                'signal_8h': None,
                'coin_data': signal['coin_data']
            }
            alerts.append(alert)
        
        return self.send_cipherb_multi_alerts(alerts)
