"""
EMA Telegram Alert System - Crossovers + Zone Touch
Using existing SMA alert format style
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

    def format_large_number(self, num: float) -> str:
        """Format large numbers"""
        if num >= 1_000_000_000:
            return f"${num/1_000_000_000:.1f}B"
        elif num >= 1_000_000:
            return f"${num/1_000_000:.0f}M"
        else:
            return f"${num/1_000:.0f}K"

    def create_chart_links(self, symbol: str) -> tuple:
        """Create TradingView and CoinGlass links for 4H"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        # 4H chart (240 minutes)
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=240"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link

    def send_ema_alerts(self, signals: List[Dict]) -> bool:
        """Send EMA alerts in existing SMA format style"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            total_alerts = len(signals)
            
            # Separate signal types
            crossover_signals = [s for s in signals if s.get('crossover_alert')]
            zone_signals = [s for s in signals if s.get('zone_alert')]
            
            message = f"""🟡 **EMA 4H SIGNALS**
📊 **{total_alerts} EMA SIGNALS DETECTED**
🕐 **{current_time}**
⏰ **Timeframe: 4H Candles**

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
                    market_cap = self.format_large_number(coin_data['market_cap'])
                    volume = self.format_large_number(coin_data['total_volume'])
                    
                    signal_emoji = "🟡" if crossover_type == 'golden_cross' else "🔴"
                    signal_name = "GOLDEN CROSS" if crossover_type == 'golden_cross' else "DEATH CROSS"
                    
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""{signal_emoji} **{signal_name}: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h)
Cap: {market_cap} | Vol: {volume}
📊 21 EMA: ${signal['ema21']:.2f}
📊 50 EMA: ${signal['ema50']:.2f}
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Add zone touch signals
            if zone_signals:
                message += "🎯 **ZONE TOUCH SIGNALS:**\n"
                
                for signal in zone_signals:
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    
                    tv_link, cg_link = self.create_chart_links(symbol)
                    
                    message += f"""🎯 **21 EMA TOUCH: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h)
📊 21 EMA: ${signal['ema21']:.2f} (Price touching)
📊 50 EMA: ${signal['ema50']:.2f}
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Add summary
            golden_count = len([s for s in crossover_signals if s.get('crossover_type') == 'golden_cross'])
            death_count = len([s for s in crossover_signals if s.get('crossover_type') == 'death_cross'])
            
            message += f"""📊 **EMA SUMMARY**
• Golden Crosses: {golden_count}
• Death Crosses: {death_count}
• Zone Touches: {len(zone_signals)}
🎯 Manual direction analysis required"""

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
            
            print(f"📱 EMA alert sent: {total_alerts} signals")
            return True

        except Exception as e:
            print(f"❌ EMA alert failed: {e}")
            return False
