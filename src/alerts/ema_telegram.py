"""
EMA Telegram Alert System - Crossovers + Zone Touch
Updated for 15M testing with better debugging
"""
import os
import requests
from datetime import datetime
from typing import List, Dict

class EMATelegramSender:
    def __init__(self, config: Dict):
        self.config = config
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('SMA_TELEGRAM_CHAT_ID')  # Using same SMA channel

    def format_price(self, price: float) -> str:
        """Format price display"""
        if price < 0.001:
            return f"${price:.8f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:.2f}"

    def create_chart_links(self, symbol: str, timeframe: str = '15') -> tuple:
        """Create TradingView and CoinGlass links"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        # Use timeframe parameter for flexibility
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe}"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link

    def send_ema_alerts(self, signals: List[Dict]) -> bool:
        """Send EMA alerts with testing format"""
        if not self.bot_token or not self.chat_id or not signals:
            print("❌ Missing bot token, chat ID, or no signals")
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            total_alerts = len(signals)
            
            # Separate signal types
            crossover_signals = [s for s in signals if s.get('crossover_alert')]
            zone_signals = [s for s in signals if s.get('zone_alert')]
            
            message = f"""🟡 **EMA 15M TEST SIGNALS**
📊 **{total_alerts} EMA TEST SIGNALS**
🕐 **{current_time}**
⏰ **Timeframe: 15M Candles (TESTING)**

"""

            # Add crossover signals
            if crossover_signals:
                message += "🔄 **CROSSOVER SIGNALS:**\n"
                
                for signal in crossover_signals:
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    crossover_type = signal['crossover_type']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    
                    signal_emoji = "🟡" if crossover_type == 'golden_cross' else "🔴"
                    signal_name = "GOLDEN CROSS" if crossover_type == 'golden_cross' else "DEATH CROSS"
                    
                    tv_link, cg_link = self.create_chart_links(symbol, '15')
                    
                    message += f"""{signal_emoji} **{signal_name}: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h)
📊 21 EMA: {self.format_price(signal['ema21'])}
📊 50 EMA: {self.format_price(signal['ema50'])}
📈 [15M Chart →]({tv_link}) | 🔥 [Liq →]({cg_link})

"""

            # Add zone touch signals
            if zone_signals:
                message += "🎯 **ZONE TOUCH SIGNALS:**\n"
                
                for signal in zone_signals:
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    
                    # Calculate distance from 21 EMA
                    distance = abs(signal['current_price'] - signal['ema21']) / signal['ema21'] * 100
                    
                    tv_link, cg_link = self.create_chart_links(symbol, '15')
                    
                    message += f"""🎯 **21 EMA TOUCH: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h)
📊 21 EMA: {self.format_price(signal['ema21'])} (Distance: {distance:.2f}%)
📊 50 EMA: {self.format_price(signal['ema50'])}
📈 [15M Chart →]({tv_link}) | 🔥 [Liq →]({cg_link})

"""

            # Add summary
            golden_count = len([s for s in crossover_signals if s.get('crossover_type') == 'golden_cross'])
            death_count = len([s for s in crossover_signals if s.get('crossover_type') == 'death_cross'])
            
            message += f"""📊 **TEST RESULTS**
• Golden Crosses: {golden_count}
• Death Crosses: {death_count}  
• Zone Touches: {len(zone_signals)}
🔧 15M Testing Mode - Logic Validation"""

            # Send to Telegram
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }

            print(f"📤 Sending to Telegram: {len(message)} characters")
            response = requests.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                print(f"📱 EMA test alert sent successfully!")
                return True
            else:
                print(f"❌ Telegram error {response.status_code}: {response.text}")
                return False

        except Exception as e:
            print(f"❌ EMA alert failed: {e}")
            import traceback
            traceback.print_exc()
            return False
