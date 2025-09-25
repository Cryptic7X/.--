"""
SMA Telegram Alert System - Crossovers + Proximity Alerts
NO RANGING ALERTS
"""
import os
import requests
from datetime import datetime
from typing import List, Dict

class SMATelegramSender:
    def __init__(self, config: Dict):
        self.config = config
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('SMA_TELEGRAM_CHAT_ID')

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

    def send_sma_batch_alert(self, signals: List[Dict], summary: Dict) -> bool:
        """Send consolidated SMA alert - NO RANGING ALERTS"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            # Build message header
            current_time = datetime.now().strftime('%H:%M:%S IST')
            total_alerts = sum(s['total_alerts'] for s in signals)
            
            message = f"""🟡 **SMA 2H SIGNALS**

📊 **{total_alerts} SMA SIGNALS DETECTED**

🕐 **{current_time}**

⏰ **Timeframe: 2H Candles**

"""

            # Group and display different types of alerts (NO RANGING)
            crossover_signals = []
            proximity_signals = []

            for signal in signals:
                symbol = signal['symbol']
                coin_data = signal['coin_data']

                # Process crossover alerts
                for alert in signal['crossover_alerts']:
                    alert_data = {
                        'symbol': symbol,
                        'coin_data': coin_data,
                        'alert': alert,
                        'type': 'crossover'
                    }
                    crossover_signals.append(alert_data)

                # Process proximity alerts (NO RANGING FILTERING)
                for alert in signal['proximity_alerts']:
                    alert_data = {
                        'symbol': symbol,
                        'coin_data': coin_data,
                        'alert': alert,
                        'type': 'proximity'
                    }
                    proximity_signals.append(alert_data)

            # Add crossover signals
            if crossover_signals:
                message += "🔄 **CROSSOVER SIGNALS:**\n"
                
                for i, signal_data in enumerate(crossover_signals, 1):
                    symbol = signal_data['symbol']
                    coin_data = signal_data['coin_data']
                    alert = signal_data['alert']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    market_cap = self.format_large_number(coin_data['market_cap'])
                    volume = self.format_large_number(coin_data['total_volume'])
                    
                    signal_emoji = "🟡" if alert['type'] == 'golden_cross' else "🔴"
                    signal_name = "GOLDEN CROSS" if alert['type'] == 'golden_cross' else "DEATH CROSS"
                    
                    tv_link, cg_link = self.create_chart_links(symbol)

                    message += f"""{signal_emoji} **{signal_name}: {symbol}**

💰 {price} ({change_24h:+.1f}% 24h)
Cap: {market_cap} | Vol: {volume}

📊 50 SMA: ${alert['sma50']:.2f}
📊 200 SMA: ${alert['sma200']:.2f}

📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Add proximity signals
            if proximity_signals:
                message += "📍 **PROXIMITY SIGNALS:**\n"
                
                for i, signal_data in enumerate(proximity_signals, 1):
                    symbol = signal_data['symbol']
                    coin_data = signal_data['coin_data']
                    alert = signal_data['alert']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    
                    signal_emoji = "🟢" if alert['type'] == 'support' else "🔴"
                    signal_name = f"{alert['type'].upper()}: {alert['level']}"
                    
                    tv_link, cg_link = self.create_chart_links(symbol)

                    message += f"""{signal_emoji} **{signal_name}: {symbol}**

💰 {price} ({change_24h:+.1f}% 24h)

📊 {alert['level']}: ${alert['sma_value']:.2f} ({alert['distance_percent']:.1f}% away)

📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Add summary (NO RANGING COUNT)
            support_count = sum(1 for s in proximity_signals if s['alert']['type'] == 'support')
            resistance_count = sum(1 for s in proximity_signals if s['alert']['type'] == 'resistance')
            
            message += f"""📊 **SMA SUMMARY**

• Total Crossovers: {summary['total_crossovers']}
• Coins Near Support: {support_count}
• Coins Near Resistance: {resistance_count}

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
            
            print(f"📱 SMA alert sent: {total_alerts} total alerts")
            return True

        except Exception as e:
            print(f"❌ SMA alert failed: {e}")
            return False
