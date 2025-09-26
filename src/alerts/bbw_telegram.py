"""
EMA Telegram Alert System - DEBUG VERSION
Simple and bulletproof with error catching
"""
import os
import requests
from datetime import datetime
from typing import List, Dict

class EMATelegramSender:
    def __init__(self, config: Dict):
        self.config = config
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('SMA_TELEGRAM_CHAT_ID')

    def format_price(self, price: float) -> str:
        """Format price safely"""
        try:
            if price < 0.001:
                return f"${price:.8f}"
            elif price < 1:
                return f"${price:.4f}"
            else:
                return f"${price:.2f}"
        except:
            return "$0.00"

    def determine_zone_info(self, ema21: float, ema50: float, current_price: float) -> Dict:
        """Simple zone logic"""
        try:
            if ema21 > ema50:
                # Bullish setup
                setup_type = "Bullish Setup"
                zone_type = "Support Zone"
                zone_range = f"[${ema50:.2f} - ${ema21:.2f}]"
            else:
                # Bearish setup  
                setup_type = "Bearish Setup"
                zone_type = "Resistance Zone" 
                zone_range = f"[${ema21:.2f} - ${ema50:.2f}]"

            return {
                'setup_type': setup_type,
                'zone_type': zone_type,
                'zone_range': zone_range
            }
        except Exception as e:
            print(f"❌ Zone calculation error: {e}")
            return {
                'setup_type': "Unknown Setup",
                'zone_type': "Unknown Zone",
                'zone_range': "[Error]"
            }

    def send_ema_alerts(self, signals: List[Dict], timeframe_minutes: int = 15) -> bool:
        """Send EMA alerts with full error handling"""
        print(f"🔍 DEBUG: Starting telegram send...")
        print(f"   Bot token: {'✅ Set' if self.bot_token else '❌ Missing'}")
        print(f"   Chat ID: {'✅ Set' if self.chat_id else '❌ Missing'}")
        print(f"   Signals: {len(signals)} total")
        
        if not self.bot_token:
            print("❌ TELEGRAM_BOT_TOKEN environment variable is missing!")
            return False
            
        if not self.chat_id:
            print("❌ SMA_TELEGRAM_CHAT_ID environment variable is missing!")
            return False
            
        if not signals:
            print("❌ No signals to send")
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            
            # Simple message format
            message = f"""🟡 **EMA 15M TEST SIGNALS**
🕐 **{current_time}**
📊 **{len(signals)} SIGNALS DETECTED**

"""

            # Add signals one by one with error handling
            for i, signal in enumerate(signals):
                try:
                    symbol = signal.get('symbol', 'UNKNOWN')
                    coin_data = signal.get('coin_data', {})
                    
                    price = self.format_price(coin_data.get('current_price', 0))
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    
                    # Determine alert type
                    if signal.get('crossover_alert'):
                        crossover_type = signal.get('crossover_type', 'unknown')
                        signal_emoji = "🟡" if crossover_type == 'golden_cross' else "🔴"
                        signal_name = "GOLDEN CROSS" if crossover_type == 'golden_cross' else "DEATH CROSS"
                        
                        message += f"""{signal_emoji} **{signal_name}: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h)

"""
                    
                    elif signal.get('zone_alert'):
                        ema21 = signal.get('ema21', 0)
                        ema50 = signal.get('ema50', 0)
                        current_price = coin_data.get('current_price', 0)
                        
                        zone_info = self.determine_zone_info(ema21, ema50, current_price)
                        signal_emoji = "🟢" if 'Support' in zone_info['zone_type'] else "🔴"
                        
                        message += f"""{signal_emoji} **{zone_info['zone_type'].upper()}: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h)
📊 {zone_info['setup_type']}: {zone_info['zone_range']}

"""
                        
                    print(f"   ✅ Added signal {i+1}: {symbol}")
                    
                except Exception as e:
                    print(f"   ❌ Error processing signal {i+1}: {e}")
                    continue

            # Add simple summary
            crossover_count = len([s for s in signals if s.get('crossover_alert')])
            zone_count = len([s for s in signals if s.get('zone_alert')])
            
            message += f"""📊 **SUMMARY**
• Crossovers: {crossover_count}
• Zone Touches: {zone_count}
🔧 Testing Mode"""

            print(f"🔍 DEBUG: Message length: {len(message)} characters")
            print(f"🔍 DEBUG: Message preview: {message[:100]}...")

            # Send to Telegram
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            
            print(f"🔍 DEBUG: Sending to {url}")
            print(f"🔍 DEBUG: Chat ID: {self.chat_id}")
            
            response = requests.post(url, json=payload, timeout=30)
            
            print(f"🔍 DEBUG: Response status: {response.status_code}")
            print(f"🔍 DEBUG: Response text: {response.text[:200]}...")
            
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get('ok'):
                    print(f"✅ Telegram message sent successfully!")
                    return True
                else:
                    print(f"❌ Telegram API error: {response_data}")
                    return False
            else:
                print(f"❌ HTTP error {response.status_code}: {response.text}")
                return False

        except Exception as e:
            print(f"❌ EMA telegram error: {e}")
            import traceback
            traceback.print_exc()
            return False
