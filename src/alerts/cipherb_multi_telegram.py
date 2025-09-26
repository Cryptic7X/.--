"""
CipherB Multi-Timeframe Telegram Alerts
Handles 2H signals, repeated signals, and 8H confirmations
"""
import os
import requests
from datetime import datetime
from typing import List, Dict

class CipherBMultiTelegramSender:
    def __init__(self, config: Dict):
        self.config = config
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('SMA_TELEGRAM_CHAT_ID')

    def format_price(self, price: float) -> str:
        if price < 0.01:
            return f"${price:.6f}"
        elif price < 1:
            return f"${price:.4f}"
        else:
            return f"${price:.2f}"

    def format_market_cap(self, market_cap: float) -> str:
        if market_cap >= 1_000_000_000:
            return f"${market_cap/1_000_000_000:.1f}B"
        elif market_cap >= 1_000_000:
            return f"${market_cap/1_000_000:.0f}M"
        else:
            return f"${market_cap/1_000:.0f}K"

    def create_chart_link(self, symbol: str, timeframe: str) -> str:
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        timeframe_minutes = {'2h': 120, '8h': 480}.get(timeframe, 120)
        return f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval={timeframe_minutes}"

    def send_multi_alerts(self, signals: List[Dict]) -> bool:
        if not self.bot_token or not self.chat_id or not signals:
            return False

        try:
            current_time = datetime.now().strftime('%H:%M:%S IST')
            
            # Group signals by type
            signal_2h = [s for s in signals if '2H' in s['alert_type']]
            signal_2h_repeated = [s for s in signal_2h if 'REPEATED' in s['alert_type']]
            signal_2h_fresh = [s for s in signal_2h if 'REPEATED' not in s['alert_type']]
            signal_8h_confirmed = [s for s in signals if '8H CONFIRMED' in s['alert_type']]

            total_signals = len(signals)

            message = f"""🎯 **CIPHERB MULTI-TIMEFRAME SIGNALS**
📊 **{total_signals} CIPHERB SIGNALS DETECTED**
🕐 **{current_time}**
⏰ **Timeframes: 2H Primary + 8H Confirmation**

"""

            # Fresh 2H signals
            if signal_2h_fresh:
                message += "⚡ **2H PRIMARY SIGNALS:**\n"
                for signal in signal_2h_fresh:
                    symbol = signal['symbol']
                    signal_type = signal['signal_type']
                    coin_data = signal['coin_data']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    market_cap = self.format_market_cap(coin_data.get('market_cap', 0))
                    
                    emoji = "🟡" if signal_type == 'BUY' else "🔴"
                    chart_link = self.create_chart_link(symbol, '2h')
                    
                    message += f"""{emoji} **2H {signal_type}: {symbol}**
💰 {price} ({change_24h:+.1f}% 24h) | Cap: {market_cap}
📊 WT1: {signal['wt1']} | WT2: {signal['wt2']}
📈 [2H Chart →]({chart_link})

"""

            # Repeated 2H signals
            if signal_2h_repeated:
                message += "🔄 **2H REPEATED SIGNALS (Now monitoring 8H):**\n"
                for signal in signal_2h_repeated:
                    symbol = signal['symbol']
                    signal_type = signal['signal_type']
                    coin_data = signal['coin_data']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    
                    emoji = "🟡" if signal_type == 'BUY' else "🔴"
                    chart_link = self.create_chart_link(symbol, '2h')
                    
                    message += f"""{emoji} **2H REPEATED: {symbol} {signal_type}**
💰 {price} ({change_24h:+.1f}% 24h)
📊 WT1: {signal['wt1']} | WT2: {signal['wt2']}
🔍 Added to 8H monitoring for confirmation
📈 [2H Chart →]({chart_link})

"""

            # 8H confirmed signals  
            if signal_8h_confirmed:
                message += "✅ **8H CONFIRMED SIGNALS:**\n"
                for signal in signal_8h_confirmed:
                    symbol = signal['symbol']
                    signal_type = signal['signal_type']
                    coin_data = signal['coin_data']
                    
                    price = self.format_price(coin_data['current_price'])
                    change_24h = coin_data.get('price_change_percentage_24h', 0)
                    market_cap = self.format_market_cap(coin_data.get('market_cap', 0))
                    
                    emoji = "🟢" if signal_type == 'BUY' else "🔻"
                    chart_8h_link = self.create_chart_link(symbol, '8h')
                    
                    message += f"""{emoji} **8H CONFIRMED: {symbol} {signal_type}**
💰 {price} ({change_24h:+.1f}% 24h) | Cap: {market_cap}
📊 8H WT1: {signal['wt1']} | 8H WT2: {signal['wt2']}
🎯 Multi-timeframe alignment confirmed
📈 [8H Chart →]({chart_8h_link})

"""

            # Summary
            buy_signals = len([s for s in signals if s['signal_type'] == 'BUY'])
            sell_signals = len([s for s in signals if s['signal_type'] == 'SELL'])
            
            message += f"""📊 **CIPHERB SUMMARY**
• 2H Primary: {len(signal_2h_fresh)} signals
• 2H Repeated: {len(signal_2h_repeated)} (now monitoring 8H)
• 8H Confirmed: {len(signal_8h_confirmed)} signals
• Direction: 🟡 {buy_signals} BUY, 🔴 {sell_signals} SELL
🎯 Multi-timeframe filtering active"""

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
            
            return True

        except Exception as e:
            print(f"❌ Telegram send failed: {e}")
            return False
