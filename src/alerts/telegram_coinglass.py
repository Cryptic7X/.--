#!/usr/bin/env python3
"""
Telegram CoinGlass Alert System
Enhanced alerts with CoinGlass liquidity heatmap integration
"""

import os
import requests
from datetime import datetime, timedelta

def get_ist_time():
    """Convert UTC to IST"""
    utc_now = datetime.utcnow()
    return utc_now + timedelta(hours=5, minutes=30)

class TelegramCoinGlassAlert:
    def __init__(self):
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.getenv('BBW_TELEGRAM_CHAT_ID')
        self.coinglass_base_url = "https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin="
        
    def validate_coinglass_link(self, symbol):
        """Validate CoinGlass link exists"""
        try:
            clean_symbol = symbol.replace('USDT', '').replace('USD', '')
            url = f"{self.coinglass_base_url}{clean_symbol}"
            
            # Quick HEAD request to check if link exists
            response = requests.head(url, timeout=5)
            if response.status_code == 200:
                return url
            else:
                return "N/A"
        except:
            return "N/A"
    
    def generate_tradingview_link(self, symbol):
        """Generate TradingView 30m chart link"""
        clean_symbol = symbol.replace('USDT', '').replace('USD', '')
        return f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=30"
    
    def format_bbw_alert(self, signals):
        """Format BBW squeeze alert with CoinGlass integration"""
        if not signals:
            return None
            
        ist_current = get_ist_time()
        
        message = f"🎯 30M BBW SQUEEZE ALERTS ({len(signals)} SIGNALS)\n"
        message += f"📅 {ist_current.strftime('%Y-%m-%d %H:%M')} IST\n"
        message += f"⚡ BBW TOUCHED LOWEST CONTRACTION\n"
        message += f"💥 SQUEEZE DETECTED\n"
        message += "=" * 40 + "\n"
        
        for i, signal in enumerate(signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            bbw_value = signal['bbw_value']
            lowest_contraction = signal['lowest_contraction']
            squeeze_strength = signal['squeeze_strength']
            
            # Format price
            if price < 0.001:
                price_fmt = f"${price:.8f}"
            elif price < 1:
                price_fmt = f"${price:.4f}"
            else:
                price_fmt = f"${price:.3f}"
            
            # Generate links
            tv_link = self.generate_tradingview_link(symbol)
            coinglass_link = self.validate_coinglass_link(symbol)
            
            message += f"{i}. {symbol}/USDT\n"
            message += f"💰 {price_fmt} ({change_24h:+.1f}%)\n"
            message += f"📊 BBW: {bbw_value:.4f}\n"
            message += f"📉 Lowest Contraction: {lowest_contraction:.4f}\n"
            message += f"🎯 Squeeze Strength: {squeeze_strength}\n"
            
            # Links with validation
            if coinglass_link != "N/A":
                message += f"📈 [Chart →]({tv_link}) | 🔥[Liq Heat →]({coinglass_link})\n"
            else:
                message += f"📈 [Chart →]({tv_link}) | 🔥Liq Heat: N/A\n"
            
            message += "\n"
        
        # Footer
        message += f"📊 SUMMARY\n"
        message += f"Total Squeezes: {len(signals)}\n"
        message += f"⏰ Next scan: 30 minutes\n"
        message += f"🎯 Manual direction analysis required"
        
        return message
    
    def send_consolidated_bbw_alert(self, signals):
        """Send consolidated BBW alert to Telegram"""
        if not signals:
            return False
            
        if not self.bot_token or not self.chat_id:
            print("❌ Telegram credentials missing")
            return False
        
        try:
            message = self.format_bbw_alert(signals)
            if not message:
                return False
                
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            data = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }
            
            response = requests.post(url, data=data, timeout=30)
            
            if response.status_code == 200:
                print(f"✅ BBW alert sent: {len(signals)} signals")
                return True
            else:
                print(f"❌ Telegram error: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Alert send failed: {e}")
            return False
