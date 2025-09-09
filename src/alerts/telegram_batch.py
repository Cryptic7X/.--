#!/usr/bin/env python3
"""
Enhanced Telegram Batch Alerts for BBW Multi-Timeframe System
Supports individual timeframe alerts and aligned alerts
"""

import os
import requests
from datetime import datetime, timedelta

def format_bbw_alert_message(signals, timeframe_type):
    """
    Format BBW signals into consolidated Telegram message
    
    Args:
        signals: List of signal dictionaries
        timeframe_type: 'individual' or 'aligned'
    """
    
    if not signals:
        return None
    
    ist_current = datetime.utcnow() + timedelta(hours=5, minutes=30)
    
    if timeframe_type == 'aligned':
        # Format aligned signals (both 30m + 1h)
        message = f"🚨 BBW SQUEEZE ALERTS - BOTH TIMEFRAMES ALIGNED\n"
        message += f"📅 {ist_current.strftime('%Y-%m-%d %H:%M IST')}\n\n"
        
        for i, signal in enumerate(signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            
            # Both timeframe data
            bbw_30m = signal['bbw_30m_value']
            lc_30m = signal['lowest_contraction_30m']
            bbw_1h = signal['bbw_1h_value'] 
            lc_1h = signal['lowest_contraction_1h']
            
            message += f"{i}. {symbol}/USDT\n"
            message += f"💰 ${price:.4f} ({change_24h:+.2f}%)\n"
            message += f"🚨 30m + 1h BBW BOTH SQUEEZED\n"
            message += f"📊 30m BBW: {bbw_30m:.4f} (LC: {lc_30m:.4f})\n"
            message += f"📊 1h BBW: {bbw_1h:.4f} (LC: {lc_1h:.4f})\n"
            
            # Both chart links
            message += f"📈 30m: https://tradingview.com/chart/?symbol=BINANCE:{symbol}USDT&interval=30\n"
            message += f"📈 1h: https://tradingview.com/chart/?symbol=BINANCE:{symbol}USDT&interval=60\n\n"
            
    else:
        # Format individual timeframe signals
        timeframe = signals[0]['timeframe']  # All signals should be same timeframe
        
        message = f"🔥 BBW SQUEEZE ALERTS - {timeframe.upper()}\n"
        message += f"📅 {ist_current.strftime('%Y-%m-%d %H:%M IST')}\n\n"
        
        for i, signal in enumerate(signals, 1):
            symbol = signal['symbol']
            price = signal['price']
            change_24h = signal['change_24h']
            bbw_value = signal['bbw_value']
            lowest_contraction = signal['lowest_contraction']
            
            message += f"{i}. {symbol}/USDT\n"
            message += f"💰 ${price:.4f} ({change_24h:+.2f}%)\n"
            message += f"⏰ Timeframe: {timeframe}\n"
            message += f"📊 BBW: {bbw_value:.4f}\n"
            message += f"📉 Lowest Contraction: {lowest_contraction:.4f}\n"
            
            # Single chart link for timeframe
            interval = '30' if timeframe == '30m' else '60'
            message += f"📈 https://tradingview.com/chart/?symbol=BINANCE:{symbol}USDT&interval={interval}\n\n"
    
    message += f"📊 Total Squeezes: {len(signals)}\n"
    message += f"🎯 Manual analysis required for direction"
    
    return message

def send_consolidated_alert(signals, alert_type='individual'):
    """
    Send consolidated BBW alert to Telegram
    
    Args:
        signals: List of BBW signal dictionaries
        alert_type: 'individual' or 'aligned'
        
    Returns:
        bool: Success status
    """
    
    if not signals:
        return False
    
    try:
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
        
        if not bot_token or not chat_id:
            print("❌ Telegram credentials not configured")
            return False
        
        message = format_bbw_alert_message(signals, alert_type)
        
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
            print(f"✅ BBW alert sent successfully ({alert_type})")
            return True
        else:
            print(f"❌ Telegram API error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Failed to send BBW alert: {e}")
        return False
