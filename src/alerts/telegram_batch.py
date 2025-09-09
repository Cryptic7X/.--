#!/usr/bin/env python3
"""
Enhanced 15-Minute BBW Telegram Alert System
Comprehensive squeeze notifications with detailed formatting
"""

import os
import requests
from datetime import datetime, timedelta

def format_15m_bbw_alert(signals):
    """Enhanced 15m BBW squeeze alert formatting"""
    
    if not signals:
        return None
    
    ist_current = datetime.utcnow() + timedelta(hours=5, minutes=30)
    
    # Enhanced header with signal count and timing
    message = f"🔥 15M BBW SQUEEZE ALERTS ({len(signals)} SIGNALS)\n"
    message += f"📅 {ist_current.strftime('%Y-%m-%d %H:%M IST')}\n"
    message += f"⚡ FAST SIGNALS - Manual direction analysis required\n\n"
    
    # Group signals by squeeze type for better organization
    tight_signals = [s for s in signals if s.get('squeeze_type') == 'TIGHT']
    normal_signals = [s for s in signals if s.get('squeeze_type') == 'NORMAL']
    loose_signals = [s for s in signals if s.get('squeeze_type') == 'LOOSE']
    
    # Format tight squeeze signals first (highest priority)
    if tight_signals:
        message += f"🎯 TIGHT SQUEEZES ({len(tight_signals)})\n"
        message += "=" * 30 + "\n"
        
        for i, signal in enumerate(tight_signals, 1):
            message += format_signal_details(signal, i)
    
    # Format normal squeeze signals
    if normal_signals:
        if tight_signals:
            message += "\n"
        message += f"🔥 NORMAL SQUEEZES ({len(normal_signals)})\n"
        message += "=" * 30 + "\n"
        
        for i, signal in enumerate(normal_signals, 1):
            message += format_signal_details(signal, i)
    
    # Format loose squeeze signals
    if loose_signals:
        if tight_signals or normal_signals:
            message += "\n"
        message += f"📊 LOOSE SQUEEZES ({len(loose_signals)})\n"
        message += "=" * 30 + "\n"
        
        for i, signal in enumerate(loose_signals, 1):
            message += format_signal_details(signal, i)
    
    # Enhanced footer with summary statistics
    message += f"\n📊 SUMMARY\n"
    message += f"Total Squeezes: {len(signals)}\n"
    
    if tight_signals:
        message += f"🎯 Tight: {len(tight_signals)}\n"
    if normal_signals:
        message += f"🔥 Normal: {len(normal_signals)}\n"
    if loose_signals:
        message += f"📊 Loose: {len(loose_signals)}\n"
    
    message += f"🎯 Direction analysis needed\n"
    message += f"⏰ Next scan: 15 minutes"
    
    return message

def format_signal_details(signal, index):
    """Format individual signal details"""
    symbol = signal['symbol']
    price = signal['price']
    change_24h = signal['change_24h']
    bbw_value = signal['bbw_value']
    lowest_contraction = signal['lowest_contraction']
    squeeze_strength = signal['squeeze_strength']
    
    # Enhanced price formatting
    if price < 0.001:
        price_str = f"${price:.8f}"
    elif price < 0.01:
        price_str = f"${price:.6f}"
    elif price < 1:
        price_str = f"${price:.4f}"
    else:
        price_str = f"${price:.2f}"
    
    details = f"{index}. {symbol}/USDT\n"
    details += f"💰 {price_str} ({change_24h:+.2f}%)\n"
    details += f"📊 BBW: {bbw_value:.4f}\n"
    details += f"📉 LC: {lowest_contraction:.4f}\n"
    details += f"🎯 Strength: {squeeze_strength:.6f}\n"
    
    # 15m TradingView chart link
    details += f"📈 https://tradingview.com/chart/?symbol=BINANCE:{symbol}USDT&interval=15\n\n"
    
    return details

def send_consolidated_alert(signals):
    """Enhanced consolidated BBW alert sender"""
    
    if not signals:
        print("📭 No signals to send")
        return False
    
    try:
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('HIGH_RISK_TELEGRAM_CHAT_ID')
        
        if not bot_token or not chat_id:
            print("❌ Telegram credentials missing")
            print("   Required: TELEGRAM_BOT_TOKEN, HIGH_RISK_TELEGRAM_CHAT_ID")
            return False
        
        message = format_15m_bbw_alert(signals)
        if not message:
            print("❌ Failed to format alert message")
            return False
        
        # Check message length and truncate if needed
        max_length = 4000
        if len(message) > max_length:
            truncated_message = message[:max_length-100] + "\n\n... (truncated)"
            print(f"⚠️ Message truncated: {len(message)} → {len(truncated_message)} chars")
            message = truncated_message
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        data = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }
        
        response = requests.post(url, data=data, timeout=30)
        
        if response.status_code == 200:
            print(f"✅ 15m BBW alert sent successfully ({len(signals)} signals)")
            return True
        else:
            print(f"❌ Telegram API error: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return False
            
    except requests.RequestException as e:
        print(f"❌ Network error sending alert: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error sending alert: {e}")
        return False

def send_test_alert():
    """Send test alert to verify Telegram setup"""
    test_signals = [{
        'symbol': 'TEST',
        'price': 1.2345,
        'change_24h': 5.67,
        'bbw_value': 2.1234,
        'lowest_contraction': 2.1200,
        'squeeze_strength': 0.0034,
        'squeeze_type': 'NORMAL'
    }]
    
    return send_consolidated_alert(test_signals)
