"""
SMA Telegram Alert System - New Concise Format
NO RANGING ALERTS - Clean and Simple
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
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=120"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link

    def send_sma_batch_alert(self, signals: List[Dict], summary: Dict) -> bool:
        """Send SMA alerts in your new concise format"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            # Build message header - EXACT format match
            current_time = datetime.now().strftime('%H:%M:%S IST')
            total_alerts = sum(s['total_alerts'] for s in signals)
            
            message = f"""🟡 SMA 2H SIGNALS
📊 {total_alerts} SMA SIGNALS DETECTED
🕐 {current_time}
⏰ Timeframe: 2H Candles

"""

            # Separate crossover and proximity signals
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

                # Process proximity alerts
                for alert in signal['proximity_alerts']:
                    alert_data = {
                        'symbol': symbol,
                        'coin_data': coin_data,
                        'alert': alert,
                        'type': 'proximity'
                    }
                    proximity_signals.append(alert_data)

            # Add crossover signals in your exact format
            if crossover_signals:
                # Group by crossover type
                golden_cross_signals = [s for s in crossover_signals if s['alert']['type'] == 'golden_cross']
                death_cross_signals = [s for s in crossover_signals if s['alert']['type'] == 'death_cross']
                
                message += "🔄 CROSSOVER SIGNALS:\n"
                
                # Golden Cross section
                if golden_cross_signals:
                    message += "🟡 GOLDEN CROSS:\n"
                    for i, signal_data in enumerate(golden_cross_signals, 1):
                        symbol = signal_data['symbol']
                        coin_data = signal_data['coin_data']
                        alert = signal_data['alert']
                        
                        price = self.format_price(coin_data['current_price'])
                        change_24h = coin_data['price_change_percentage_24h']
                        market_cap = self.format_large_number(coin_data['market_cap'])
                        volume = self.format_large_number(coin_data['total_volume'])
                        
                        tv_link, cg_link = self.create_chart_links(symbol)

                        message += f"""{i}. {symbol} |💰 {price} | ({change_24h:+.1f}% 24h)
   Cap: {market_cap} | Vol: {volume}
   📊 50 SMA: {alert['sma50']:.2f}
   📊 200 SMA: {alert['sma200']:.2f}
   📈  [Chart →]({tv_link}) | 🔥  [Liq Heat →]({cg_link})

"""

                # Death Cross section  
                if death_cross_signals:
                    message += "🔴 DEATH CROSS:\n"
                    for i, signal_data in enumerate(death_cross_signals, 1):
                        symbol = signal_data['symbol']
                        coin_data = signal_data['coin_data']
                        alert = signal_data['alert']
                        
                        price = self.format_price(coin_data['current_price'])
                        change_24h = coin_data['price_change_percentage_24h']
                        market_cap = self.format_large_number(coin_data['market_cap'])
                        volume = self.format_large_number(coin_data['total_volume'])
                        
                        tv_link, cg_link = self.create_chart_links(symbol)

                        message += f"""{i}. {symbol} |💰 {price} | ({change_24h:+.1f}% 24h)
   Cap: {market_cap} | Vol: {volume}
   📊 50 SMA: {alert['sma50']:.2f}
   📊 200 SMA: {alert['sma200']:.2f}
   📈  [Chart →]({tv_link}) | 🔥  [Liq Heat →]({cg_link})

"""

            # Add proximity signals in similar concise format
            if proximity_signals:
                # Group by support/resistance
                support_signals = [s for s in proximity_signals if s['alert']['type'] == 'support']
                resistance_signals = [s for s in proximity_signals if s['alert']['type'] == 'resistance']
                
                message += "📍 PROXIMITY SIGNALS:\n"
                
                # Support section
                if support_signals:
                    message += "🟢 SUPPORT:\n"
                    for i, signal_data in enumerate(support_signals, 1):
                        symbol = signal_data['symbol']
                        coin_data = signal_data['coin_data']
                        alert = signal_data['alert']
                        
                        price = self.format_price(coin_data['current_price'])
                        change_24h = coin_data['price_change_percentage_24h']
                        
                        tv_link, cg_link = self.create_chart_links(symbol)

                        message += f"""{i}. {symbol} |💰 {price} | ({change_24h:+.1f}% 24h)
   📊 {alert['level']}: {alert['sma_value']:.2f} ({alert['distance_percent']:.1f}% away)
   📈  [Chart →]({tv_link}) | 🔥  [Liq Heat →]({cg_link})

"""

                # Resistance section
                if resistance_signals:
                    message += "🔴 RESISTANCE:\n"
                    for i, signal_data in enumerate(resistance_signals, 1):
                        symbol = signal_data['symbol']
                        coin_data = signal_data['coin_data']
                        alert = signal_data['alert']
                        
                        price = self.format_price(coin_data['current_price'])
                        change_24h = coin_data['price_change_percentage_24h']
                        
                        tv_link, cg_link = self.create_chart_links(symbol)

                        message += f"""{i}. {symbol} |💰 {price} | ({change_24h:+.1f}% 24h)
   📊 {alert['level']}: {alert['sma_value']:.2f} ({alert['distance_percent']:.1f}% away)
   📈  [Chart →]({tv_link}) | 🔥  [Liq Heat →]({cg_link})

"""

            # Add summary - EXACT format match
            support_count = sum(1 for s in proximity_signals if s['alert']['type'] == 'support')
            resistance_count = sum(1 for s in proximity_signals if s['alert']['type'] == 'resistance')
            
            message += f"""📊 SMA SUMMARY
• Total Crossovers: {summary['total_crossovers']}
• Coins Near Support: {support_count}
• Coins Near Resistance: {resistance_count}"""

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
            
            print(f"📱 SMA alert sent: {total_alerts} total alerts")
            return True

        except Exception as e:
            print(f"❌ SMA alert failed: {e}")
            return False
