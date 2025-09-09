#!/usr/bin/env python3
"""
15-Minute BBW Telegram Alert System
Optimized for fast 15m squeeze notifications
"""

import os
import requests
from datetime import datetime, timedelta

def format_15m_bbw_alert(signals):
    """Format 15m BBW squeeze alerts for Telegram"""
    
    if not signals:
        return None
    
    ist_current = datetime.utcnow() + timedelta(hours=5, minutes=30)
    
    message = f"🔥 15M BBW SQUEEZE ALERTS\n"
    message += f"📅 {ist_current.strftime('%Y-%m-%d %H:%M IST')}\n"
    message += f"⚡ FAST SIGNALS - Manual analysis required\n\n"
    
    for i, signal in enumerate(signals, 1):
        symbol = signal['symbol']
        price = signal['price']
        change_24h = signal['change_24h']
        bbw_value = signal['bbw_value']
        lowest_contraction = signal['lowest_contraction']
        squeeze_strength = signal['squeeze_strength']
        
        message += f"{i}. {symbol}/USDT\n"
        message += f"💰 ${price:.6f} ({change_24h:+.2f}%)\n"
        message += f"📊 BBW: {bbw_value:.4f}\n"
        message += f"📉 Lowest: {lowest_contraction:.4f}\n"
        message += f"🎯 Strength: {squeeze_strength:.6f}\n"
        
        # 15m TradingView chart link
        message += f"📈 https://tradingview.com/chart/?symbol=BINANCE:{symbol}USDT&interval=15\n\n"
    
    message += f"📊 Total 15m Squeezes: {len(signals)}\n"
    message += f"🎯 Direction analysis needed\n"
    message += f"⏰ Next scan: 15 minutes"
    
    return message

def send_consolidated_alert(signals):
    """Send 15m BBW alerts to Telegram"""
    
    if not signals:
        return False
    
    try:
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
        
        if not bot_token or not chat_id:
            print("❌ Telegram credentials missing")
            return False
        
        message = format_15m_bbw_alert(signals)
        if not message:
            return False
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        data = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, data=data, timeout=30)
        
        if response.status_code == 200:
            print(f"✅ 15m BBW alert sent ({len(signals)} signals)")
            return True
        else:
            print(f"❌ Telegram error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Alert send failed: {e}")
        return False
