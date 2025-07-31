from trading_system import Database

if __name__ == "__main__":
    print("初始化交易系统数据库...")
    db = Database()
    
    # 添加示例资产
    cursor = db.conn.cursor()
    assets = [
        (1, 'BTC', 'Bitcoin', 'crypto', 'crypto', 'Binance'),
        (2, 'ETH', 'Ethereum', 'crypto', 'crypto', 'Binance'),
        (3, 'XAU', 'Gold', 'precious_metals', 'metal', 'Bitpanda'),
        (4, 'AAPL', 'Apple Inc.', 'stocks', 'technology', 'XNAS'),
        (5, 'MSFT', 'Microsoft Corporation', 'stocks', 'technology', 'XNAS'),
        (6, 'SPY', 'SPDR S&P 500 ETF', 'etf', 'index', 'ARCX')
    ]
    
    cursor.executemany(
        "INSERT OR IGNORE INTO assets (id, symbol, name, asset_type, category, exchange) VALUES (?, ?, ?, ?, ?, ?)",
        assets
    )
    db.conn.commit()
    
    print("数据库初始化完成！")
    print("创建了6个示例资产")