"""
Enhanced Telegram Batch Alert System with CoinGlass Integration
Fixed TradingView and CoinGlass link formats
"""

import os
import requests
from datetime import datetime

def generate_chart_link(symbol):
    """Generate TradingView chart link for 1-hour timeframe with correct format"""
    # Clean symbol: Remove /USDT, /USD, USDT, USD and slashes
    clean_symbol = symbol.replace('USDT', '').replace('USD', '').replace('/', '')
    
    # Format: SYMBOLUSDT (e.g., QNTUSDT instead of QNT/USDT)
    return f"https://www.tradingview.com/chart/?symbol={clean_symbol}USDT&interval=60"

def generate_coinglass_link(symbol):
    """Generate CoinGlass liquidation heatmap link with correct format"""
    # Clean symbol: Remove /USDT, /USD, USDT, USD and slashes  
    clean_symbol = symbol.replace('USDT', '').replace('USD', '').replace('/', '')
    
    # Format: Add &type=pair parameter
    return f"https://www.coinglass.com/pro/futures/LiquidationHeatMapNew?coin={clean_symbol}&type=pair"

def format_batch_alert_message(signals, indicator_type, timeframe):
    """Format batch alert message with corrected CoinGlass integration"""
    
    if not signals:
        return "No signals to report"
    
    # Header
    ist_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    message = f"""🎯 {timeframe} {indicator_type} SQUEEZE ALERTS ({len(signals)} SIGNALS)
📅 {ist_time} IST
⚡ BBW TOUCHED LOWEST CONTRACTION
💥 SQUEEZE DETECTED
================================
"""
    
    # Individual signals
    for i, signal in enumerate(signals, 1):
        chart_link = generate_chart_link(signal['symbol'])
        coinglass_link = generate_coinglass_link(signal['symbol'])
        
        message += f"""
{i}. {signal['symbol']}
💰 ${signal['price']:,.2f} ({signal['change_24h']:+.1f}%)
📊 BBW: {signal['bbw_value']:.4f}
📉 Lowest Contraction: {signal['lowest_contraction']:.4f}
🎯 Squeeze Strength: {signal['strength']}
📈 [Chart →]({chart_link}) | 🔥[Liq Heat →]({coinglass_link})
"""
    
    # Summary
    message += f"""
📊 SUMMARY
Total Squeezes: {len(signals)}
🎯 Manual direction analysis required"""
    
    return message

def send_telegram_message(message):
    """Send message to Telegram"""
    bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('BBW_TELEGRAM_CHAT_ID')
    
    if not bot_token or not chat_id:
        print("❌ Telegram credentials not configured")
        return False
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': True
    }
    
    try:
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            print("✅ Telegram alert sent successfully")
            return True
        else:
            print(f"❌ Telegram API error: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Failed to send Telegram message: {e}")
        return False

def send_batch_telegram_alert(signals, indicator_type="BBW", timeframe="1H"):
    """Send batch Telegram alert with all signals"""
    
    if not signals:
        print("ℹ️ No signals to send")
        return
    
    message = format_batch_alert_message(signals, indicator_type, timeframe)
    
    # Split message if too long (Telegram limit: 4096 characters)
    max_length = 4000
    
    if len(message) <= max_length:
        send_telegram_message(message)
    else:
        # Split into multiple messages
        parts = []
        current_part = ""
        
        for line in message.split('\n'):
            if len(current_part + line + '\n') <= max_length:
                current_part += line + '\n'
            else:
                if current_part:
                    parts.append(current_part)
                current_part = line + '\n'
        
        if current_part:
            parts.append(current_part)
        
        # Send all parts
        for i, part in enumerate(parts, 1):
            header = f"📨 Part {i}/{len(parts)}\n" if len(parts) > 1 else ""
            send_telegram_message(header + part)

# Test function for debugging links
def test_link_formats():
    """Test function to verify link formats"""
    test_symbols = ['QNT/USDT', 'QNTUSDT', 'BTC/USDT', 'BTCUSDT']
    
    print("Testing link formats:")
    print("=" * 50)
    
    for symbol in test_symbols:
        chart_link = generate_chart_link(symbol)
        coinglass_link = generate_coinglass_link(symbol)
        
        print(f"\nSymbol: {symbol}")
        print(f"TradingView: {chart_link}")
        print(f"CoinGlass: {coinglass_link}")

if __name__ == "__main__":
    # Run tests
    test_link_formats()
