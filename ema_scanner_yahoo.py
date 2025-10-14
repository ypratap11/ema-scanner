#!/usr/bin/env python3
"""
EMA Scanner - Yahoo Finance Only
Scans S&P 500, NASDAQ-100, and Top 100 Crypto
"""

import json
import time
from datetime import datetime
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


def calculate_ema(prices, period):
    """Calculate Exponential Moving Average"""
    if len(prices) < period:
        return 0.0

    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return ema


def fetch_yahoo_data(symbol_info):
    """Fetch stock/crypto data from Yahoo Finance"""
    symbol = symbol_info['symbol']
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {'interval': '1d', 'range': '1y'}
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

        response = requests.get(url, params=params, headers=headers, timeout=10)

        if response.status_code != 200:
            return None

        data = response.json()

        if 'chart' not in data or 'result' not in data['chart'] or not data['chart']['result']:
            return None

        result = data['chart']['result'][0]

        if 'indicators' not in result or 'quote' not in result['indicators']:
            return None

        closes = result['indicators']['quote'][0]['close']
        closes = [c for c in closes if c is not None]

        if len(closes) < 50:
            return None

        current_price = closes[-1]
        ema50 = calculate_ema(closes[-50:], 50)
        ema200 = calculate_ema(closes[-200:], 200) if len(closes) >= 200 else calculate_ema(closes, len(closes))

        return {
            **symbol_info,
            'currentPrice': current_price,
            'ema50': ema50,
            'ema200': ema200,
            'ema50Distance': ((current_price - ema50) / ema50) * 100,
            'ema200Distance': ((current_price - ema200) / ema200) * 100,
        }

    except Exception as e:
        return None


def get_sp500_symbols():
    """Get S&P 500 symbols from Wikipedia"""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        tables = requests.get(url).text

        # Simple parsing - extract symbols from the table
        import re
        symbols = re.findall(r'<td><a[^>]*>([A-Z\.]+)</a>', tables)

        return [{'symbol': s, 'type': 'stock', 'name': s, 'index': 'SPY'} for s in symbols[:500]]
    except:
        # Fallback to major S&P 500 stocks
        major_sp500 = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B', 'LLY', 'AVGO',
            'JPM', 'V', 'UNH', 'XOM', 'WMT', 'MA', 'PG', 'JNJ', 'HD', 'COST',
            'NFLX', 'BAC', 'CRM', 'ABBV', 'CVX', 'KO', 'MRK', 'AMD', 'PEP', 'ADBE',
            'TMO', 'ACN', 'MCD', 'CSCO', 'ABT', 'WFC', 'LIN', 'ORCL', 'DIS', 'GE',
            'CMCSA', 'PM', 'QCOM', 'IBM', 'VZ', 'INTC', 'TXN', 'NOW', 'AMGN', 'CAT'
        ]
        return [{'symbol': s, 'type': 'stock', 'name': s, 'index': 'SPY'} for s in major_sp500]


def get_nasdaq100_symbols():
    """Get NASDAQ-100 symbols"""
    # Major NASDAQ-100 stocks
    nasdaq100 = [
        'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'NVDA', 'META', 'TSLA', 'AVGO', 'COST',
        'ASML', 'NFLX', 'AMD', 'ADBE', 'PEP', 'CSCO', 'TMUS', 'LIN', 'TXN', 'QCOM',
        'INTU', 'CMCSA', 'AMGN', 'HON', 'AMAT', 'SBUX', 'PANW', 'BKNG', 'ISRG', 'ADP',
        'GILD', 'VRTX', 'ADI', 'MU', 'REGN', 'LRCX', 'MDLZ', 'INTC', 'PYPL', 'KLAC',
        'SNPS', 'CDNS', 'MELI', 'CTAS', 'CRWD', 'MAR', 'PDD', 'NXPI', 'MRVL', 'FTNT',
        'ORLY', 'CSX', 'ADSK', 'DASH', 'ABNB', 'ROP', 'WDAY', 'PCAR', 'MNST', 'AEP',
        'CPRT', 'CHTR', 'ROST', 'ODFL', 'PAYX', 'FAST', 'KDP', 'MCHP', 'EA', 'BKR',
        'DXCM', 'VRSK', 'AZN', 'GEHC', 'CTSH', 'XEL', 'KHC', 'EXC', 'TEAM', 'LULU',
        'IDXX', 'CCEP', 'CSGP', 'ZS', 'ANSS', 'TTWO', 'FANG', 'ON', 'CDW', 'BIIB',
        'WBD', 'GFS', 'DDOG', 'MDB', 'ILMN', 'MRNA', 'WBA', 'ARM', 'DLTR', 'SMCI'
    ]
    return [{'symbol': s, 'type': 'stock', 'name': s, 'index': 'NASDAQ'} for s in nasdaq100]


def get_top_crypto():
    """Get top 100 crypto by market cap"""
    # Top 100 crypto that are available on Yahoo Finance
    top_crypto = [
        ('BTC-USD', 'Bitcoin'), ('ETH-USD', 'Ethereum'), ('BNB-USD', 'BNB'),
        ('SOL-USD', 'Solana'), ('XRP-USD', 'Ripple'), ('USDC-USD', 'USD Coin'),
        ('ADA-USD', 'Cardano'), ('AVAX-USD', 'Avalanche'), ('DOGE-USD', 'Dogecoin'),
        ('TRX-USD', 'TRON'), ('DOT-USD', 'Polkadot'), ('MATIC-USD', 'Polygon'),
        ('LTC-USD', 'Litecoin'), ('SHIB-USD', 'Shiba Inu'), ('UNI-USD', 'Uniswap'),
        ('LINK-USD', 'Chainlink'), ('ATOM-USD', 'Cosmos'), ('XLM-USD', 'Stellar'),
        ('XMR-USD', 'Monero'), ('BCH-USD', 'Bitcoin Cash'), ('ALGO-USD', 'Algorand'),
        ('FIL-USD', 'Filecoin'), ('ETC-USD', 'Ethereum Classic'), ('HBAR-USD', 'Hedera'),
        ('AAVE-USD', 'Aave'), ('VET-USD', 'VeChain'), ('THETA-USD', 'Theta'),
        ('ICP-USD', 'Internet Computer'), ('AXS-USD', 'Axie Infinity'), ('SAND-USD', 'Sandbox'),
        ('MANA-USD', 'Decentraland'), ('GRT-USD', 'The Graph'), ('SNX-USD', 'Synthetix'),
        ('FTM-USD', 'Fantom'), ('EGLD-USD', 'MultiversX'), ('FLOW-USD', 'Flow'),
        ('XTZ-USD', 'Tezos'), ('KLAY-USD', 'Klaytn'), ('CHZ-USD', 'Chiliz'),
        ('ZEC-USD', 'Zcash'), ('BAT-USD', 'Basic Attention'), ('ENJ-USD', 'Enjin'),
        ('1INCH-USD', '1inch'), ('CRV-USD', 'Curve'), ('LRC-USD', 'Loopring'),
        ('COMP-USD', 'Compound'), ('YFI-USD', 'Yearn.finance'), ('SUSHI-USD', 'SushiSwap'),
        ('ZRX-USD', '0x'), ('OMG-USD', 'OMG Network'), ('BAL-USD', 'Balancer'),
        ('REN-USD', 'Ren'), ('BNT-USD', 'Bancor'), ('STORJ-USD', 'Storj'),
        ('ANT-USD', 'Aragon'), ('KNC-USD', 'Kyber Network'), ('REP-USD', 'Augur'),
        ('NMR-USD', 'Numeraire'), ('RLC-USD', 'iExec RLC'), ('OCEAN-USD', 'Ocean Protocol'),
        ('ANKR-USD', 'Ankr'), ('SKL-USD', 'SKALE'), ('API3-USD', 'API3'),
        ('AUDIO-USD', 'Audius'), ('BAND-USD', 'Band Protocol'), ('COTI-USD', 'COTI'),
        ('CTSI-USD', 'Cartesi'), ('CELR-USD', 'Celer Network'), ('FET-USD', 'Fetch.ai'),
        ('INJ-USD', 'Injective'), ('MASK-USD', 'Mask Network'), ('NKN-USD', 'NKN'),
        ('NU-USD', 'NuCypher'), ('OGN-USD', 'Origin Protocol'), ('PERP-USD', 'Perpetual'),
        ('POLS-USD', 'Polkastarter'), ('POLY-USD', 'Polymath'), ('QNT-USD', 'Quant'),
        ('RARI-USD', 'Rarible'), ('RSR-USD', 'Reserve Rights'), ('SKL-USD', 'SKALE'),
        ('SRM-USD', 'Serum'), ('SUPER-USD', 'SuperFarm'), ('SXP-USD', 'Swipe'),
        ('TRB-USD', 'Tellor'), ('UMA-USD', 'UMA'), ('WNXM-USD', 'Wrapped NXM'),
        ('XYO-USD', 'XYO'), ('YGG-USD', 'Yield Guild'), ('APE-USD', 'ApeCoin'),
        ('GMT-USD', 'STEPN'), ('GAL-USD', 'Galxe'), ('OP-USD', 'Optimism'),
        ('ARB-USD', 'Arbitrum'), ('BLUR-USD', 'Blur'), ('PEPE-USD', 'Pepe'),
        ('STX-USD', 'Stacks'), ('IMX-USD', 'Immutable'), ('RNDR-USD', 'Render'),
        ('LDO-USD', 'Lido DAO'), ('RPL-USD', 'Rocket Pool'), ('FXS-USD', 'Frax Share')
    ]
    return [{'symbol': s, 'type': 'crypto', 'name': n, 'index': 'CRYPTO'} for s, n in top_crypto]


def main():
    """Main execution"""

    print("\n" + "=" * 70)
    print("🚀 COMPREHENSIVE EMA SCANNER")
    print("=" * 70)
    print("📊 S&P 500 Stocks")
    print("📊 NASDAQ-100 Stocks")
    print("🪙 Top 100 Cryptocurrencies")
    print("=" * 70 + "\n")

    # Gather all symbols
    print("📋 Gathering symbols...")
    sp500 = get_sp500_symbols()
    nasdaq = get_nasdaq100_symbols()
    crypto = get_top_crypto()

    # Combine and deduplicate
    all_symbols = sp500 + nasdaq + crypto

    # Remove duplicates based on symbol
    seen = set()
    unique_symbols = []
    for s in all_symbols:
        if s['symbol'] not in seen:
            seen.add(s['symbol'])
            unique_symbols.append(s)

    print(f"✅ Found {len(unique_symbols)} unique symbols")
    print(f"   - {len([s for s in unique_symbols if s['type'] == 'stock'])} stocks")
    print(f"   - {len([s for s in unique_symbols if s['type'] == 'crypto'])} crypto\n")

    print("🔍 Scanning (this will take a few minutes)...\n")

    results = []
    failed = 0

    # Use ThreadPoolExecutor for parallel fetching
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_symbol = {executor.submit(fetch_yahoo_data, s): s for s in unique_symbols}

        for i, future in enumerate(as_completed(future_to_symbol), 1):
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(unique_symbols)} ({(i / len(unique_symbols) * 100):.1f}%)")

            try:
                result = future.result()
                if result:
                    results.append(result)
                else:
                    failed += 1
            except Exception as e:
                failed += 1

    # Save results
    output = {
        'lastUpdate': datetime.utcnow().isoformat() + 'Z',
        'source': 'Yahoo Finance (Free API)',
        'totalScanned': len(unique_symbols),
        'successful': len(results),
        'failed': failed,
        'data': results
    }

    with open('ema_scan_results.json', 'w') as f:
        json.dump(output, f, indent=2)

    # Summary
    print("\n" + "=" * 70)
    print("SCAN SUMMARY")
    print("=" * 70)
    print(f"✅ Successfully scanned: {len(results)}/{len(unique_symbols)} symbols")
    print(f"❌ Failed: {failed}")

    if results:
        # Stock summary
        stocks = [r for r in results if r['type'] == 'stock']
        near_50_stocks = [r for r in stocks if -3 < r['ema50Distance'] < 0]
        above_50_stocks = [r for r in stocks if r['ema50Distance'] > 0]

        print(f"\n📊 STOCKS ({len(stocks)} total):")
        print(f"   🟡 Near 50 EMA breakout: {len(near_50_stocks)}")
        print(f"   🟢 Above 50 EMA: {len(above_50_stocks)}")

        # Crypto summary
        cryptos = [r for r in results if r['type'] == 'crypto']
        near_50_crypto = [r for r in cryptos if -3 < r['ema50Distance'] < 0]
        above_50_crypto = [r for r in cryptos if r['ema50Distance'] > 0]

        print(f"\n🪙 CRYPTO ({len(cryptos)} total):")
        print(f"   🟡 Near 50 EMA breakout: {len(near_50_crypto)}")
        print(f"   🟢 Above 50 EMA: {len(above_50_crypto)}")

        # Top near-breakout opportunities
        all_near = [r for r in results if -3 < r['ema50Distance'] < 0]
        all_near.sort(key=lambda x: abs(x['ema50Distance']))

        if all_near:
            print(f"\n🔥 TOP 10 NEAREST TO 50 EMA BREAKOUT:")
            for r in all_near[:10]:
                print(f"   {r['name']} ({r['symbol']}): {r['ema50Distance']:.2f}%")

    print("\n" + "=" * 70)
    print(f"📁 Results saved to: ema_scan_results.json")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()