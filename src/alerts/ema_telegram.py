"""
EMA Telegram Alert System - EXACT SMA Format
Crossovers + Zone Touch with Support/Resistance Zone Logic
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

    def create_chart_links(self, symbol: str, timeframe_minutes: int = 240) -> tuple:
        """Create TradingView and CoinGlass links - Default 4H for production"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        tv_link = f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe_minutes}"
        cg_link = f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}"
        return tv_link, cg_link

    def determine_zone_info(self, ema21: float, ema50: float, current_price: float) -> Dict:
        """
        Determine zone setup and current price position:
        - If 21 EMA > 50 EMA: Bullish Setup → Support Zone = [50 EMA, 21 EMA]
        - If 50 EMA > 21 EMA: Bearish Setup → Resistance Zone = [21 EMA, 50 EMA]
        """
        if ema21 > ema50:
            # Bullish setup
            zone_bottom = ema50
            zone_top = ema21
            setup_type = "Bullish Setup"
            zone_type = "Support Zone"
            zone_range = f"[${zone_bottom:.2f} - ${zone_top:.2f}]"
        else:
            # Bearish setup  
            zone_bottom = ema21
            zone_top = ema50
            setup_type = "Bearish Setup"
            zone_type = "Resistance Zone" 
            zone_range = f"[${zone_bottom:.2f} - ${zone_top:.2f}]"

        # Check if price is in zone
        is_in_zone = zone_bottom <= current_price <= zone_top

        return {
            'setup_type': setup_type,
            'zone_type': zone_type,
            'zone_range': zone_range,
            'zone_bottom': zone_bottom,
            'zone_top': zone_top,
            'is_in_zone': is_in_zone
        }

    def send_ema_alerts(self, signals: List[Dict], timeframe_minutes: int = 15) -> bool:
        """Send EMA alerts in EXACT SMA format with zone logic"""
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            total_alerts = len(signals)
            
            # Determine timeframe display
            if timeframe_minutes == 15:
                tf_display = "15M Candles (TESTING)"
            elif timeframe_minutes == 240:
                tf_display = "4H Candles"
            else:
                tf_display = f"{timeframe_minutes}M Candles"

            # Build message header - EXACT SMA FORMAT
            message = f"""🟡 **EMA {tf_display.split()[0]} SIGNALS**
📊 **{total_alerts} EMA SIGNALS DETECTED**
🕐 **{current_time}**
⏰ **Timeframe: {tf_display}**

"""

            # Separate signal types
            crossover_signals = [s for s in signals if s.get('crossover_alert')]
            zone_signals = [s for s in signals if s.get('zone_alert')]

            # Add crossover signals - EXACT SMA FORMAT
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
                    
                    tv_link, cg_link = self.create_chart_links(symbol, timeframe_minutes)
                    
                    message += f"""{signal_emoji} **{signal_name}: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h)
Cap: {market_cap} | Vol: {volume}
📊 21 EMA: {self.format_price(signal['ema21'])}
📊 50 EMA: {self.format_price(signal['ema50'])}
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Add zone touch signals - EXACT SMA PROXIMITY FORMAT with Zone Logic
            if zone_signals:
                message += "🎯 **ZONE TOUCH SIGNALS:**\n"
                
                for signal in zone_signals:
                    symbol = signal['symbol']
                    coin_data = signal['coin_data']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data['price_change_percentage_24h']
                    
                    # Get zone information
                    zone_info = self.determine_zone_info(
                        signal['ema21'], 
                        signal['ema50'], 
                        coin_data['current_price']
                    )
                    
                    # Signal emoji based on zone type
                    signal_emoji = "🟢" if zone_info['zone_type'] == 'Support Zone' else "🔴"
                    
                    tv_link, cg_link = self.create_chart_links(symbol, timeframe_minutes)
                    
                    message += f"""{signal_emoji} **{zone_info['zone_type'].upper()}: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h)
📊 {zone_info['setup_type']}: {zone_info['zone_range']}
🎯 Price in {zone_info['zone_type']}
📈 [Chart →]({tv_link}) | 🔥 [Liq Heat →]({cg_link})

"""

            # Add summary - EXACT SMA FORMAT
            golden_count = len([s for s in crossover_signals if s.get('crossover_type') == 'golden_cross'])
            death_count = len([s for s in crossover_signals if s.get('crossover_type') == 'death_cross'])
            support_count = len([s for s in zone_signals if self.determine_zone_info(s['ema21'], s['ema50'], s['current_price'])['zone_type'] == 'Support Zone'])
            resistance_count = len([s for s in zone_signals if self.determine_zone_info(s['ema21'], s['ema50'], s['current_price'])['zone_type'] == 'Resistance Zone'])
            
            message += f"""📊 **EMA SUMMARY**
• Total Crossovers: {len(crossover_signals)} (🟡 {golden_count} Golden, 🔴 {death_count} Death)
• Coins in Support Zone: {support_count}
• Coins in Resistance Zone: {resistance_count}
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
            
            print(f"📱 EMA alert sent: {total_alerts} total signals")
            return True

        except Exception as e:
            print(f"❌ EMA alert failed: {e}")
            import traceback
            traceback.print_exc()
            return False

