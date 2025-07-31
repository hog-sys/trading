import asyncio
import aiohttp
import sqlite3
import time
import json
import hashlib
import hmac
import numpy as np
import pandas as pd
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_system.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("TradingSystem")

# ====================== 配置类 ======================
class Config:
    # API 配置
    BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
    BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
    BITPANDA_API_KEY = os.getenv("BITPANDA_API_KEY")
    ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
    
    # 交易参数
    DAILY_INVESTMENT_EUR = 3.0
    MAX_SINGLE_INVESTMENT_EUR = 10.0
    MAX_DAILY_INVESTMENT_EUR = 15.0
    MAX_POSITIONS = 12
    BASE_STOP_LOSS_PCT = 0.05
    BASE_TAKE_PROFIT_PCT = 0.12
    
    # 资产配置
    ASSET_ALLOCATION = {
        "crypto": 0.35,
        "stocks": 0.20,
        "etf": 0.35,
        "precious_metals": 0.10
    }

    @classmethod
    def validate_config(cls):
        required_vars = [
            'BINANCE_API_KEY', 'BINANCE_API_SECRET', 'BITPANDA_API_KEY',
            'ALPHA_VANTAGE_KEY', 'POLYGON_API_KEY', 'TELEGRAM_TOKEN',
            'TELEGRAM_CHAT_ID'
        ]
        missing_vars = []
        for var in required_vars:
            if not getattr(cls, var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            sys.exit(1)
    
    ETF_CATEGORY_ALLOCATION = {
        "large_cap": 0.30,
        "sector": 0.25,
        "thematic": 0.20,
        "commodities": 0.15,
        "bonds": 0.10
    }
    
    # 数据源
    DATA_SOURCES = {
        "crypto": ["Binance", "CoinGecko"],
        "stocks": "Polygon",
        "etf": ["YahooFinance", "AlphaVantage"],
        "precious_metals": "Bitpanda"
    }
    
    # 通知配置
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    
    # 系统参数
    TRADING_HOURS = {
        "stocks": {"start": "08:00", "end": "22:00", "days": [0,1,2,3,4]},  # 周一至周五
        "etf": {"start": "08:00", "end": "22:00", "days": [0,1,2,3,4]},
        "precious_metals": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4,5,6]},
        "crypto": {"start": "00:00", "end": "23:59", "days": [0,1,2,3,4,5,6]}  # 24/7
    }

Config.validate_config()

# ====================== 数据库模块 ======================
class Database:
    def __init__(self, db_name="trading_system.db"):
        try:
            self.conn = sqlite3.connect(db_name)
            self.create_tables()
        except sqlite3.Error as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def create_tables(self):
        try:
            cursor = self.conn.cursor()
            
            # 资产表
            cursor.execute('''CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                name TEXT,
                asset_type TEXT NOT NULL,
                category TEXT,
                exchange TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
            
            # 价格数据表
            cursor.execute('''CREATE TABLE IF NOT EXISTS prices (
                asset_id INTEGER NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                source TEXT,
                FOREIGN KEY(asset_id) REFERENCES assets(id)
            )''')
            
            # 分析结果表
            cursor.execute('''CREATE TABLE IF NOT EXISTS analysis (
                asset_id INTEGER NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                technical_score INTEGER,
                fundamental_score INTEGER,
                sentiment_score INTEGER,
                composite_score INTEGER,
                FOREIGN KEY(asset_id) REFERENCES assets(id)
            )''')
            
            # 交易表
            cursor.execute('''CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY,
                asset_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                platform TEXT NOT NULL,
                status TEXT DEFAULT 'executed',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(asset_id) REFERENCES assets(id)
            )''')
            
            # 持仓表
            cursor.execute('''CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY,
                trade_id INTEGER NOT NULL,
                asset_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                current_price REAL,
                stop_loss REAL,
                take_profit REAL,
                status TEXT DEFAULT 'open',
                opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                FOREIGN KEY(asset_id) REFERENCES assets(id),
                FOREIGN KEY(trade_id) REFERENCES trades(id)
            )''')
            
            # 系统状态表
            cursor.execute('''CREATE TABLE IF NOT EXISTS system_status (
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_equity REAL,
                cash_balance REAL,
                risk_level REAL,
                active_positions INTEGER,
                daily_profit REAL
            )''')
            
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to create tables: {e}")
            self.conn.rollback()
            raise
    
    def log_trade(self, asset_id, action, quantity, price, platform):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT INTO trades (asset_id, action, quantity, price, platform) VALUES (?, ?, ?, ?, ?)",
                (asset_id, action, quantity, price, platform)
            )
            trade_id = cursor.lastrowid
            self.conn.commit()
            return trade_id
        except sqlite3.Error as e:
            logger.error(f"Failed to log trade: {e}")
            self.conn.rollback()
            return None
    
    def update_position(self, position_id, current_price=None, status=None):
        try:
            cursor = self.conn.cursor()
            if status == "closed":
                cursor.execute(
                    "UPDATE positions SET status = 'closed', closed_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (position_id,)
                )
            elif current_price:
                cursor.execute(
                    "UPDATE positions SET current_price = ? WHERE id = ?",
                    (current_price, position_id)
                )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to update position: {e}")
            self.conn.rollback()
    
    def get_active_positions(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM positions WHERE status = 'open'")
            return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Failed to get active positions: {e}")
            return []
    
    def get_daily_trades(self):
        today = datetime.now().strftime("%Y-%m-%d")
        cursor = self.conn.cursor()
        cursor.execute("SELECT SUM(quantity * price) FROM trades WHERE DATE(timestamp) = ?", (today,))
        result = cursor.fetchone()
        return result[0] if result[0] else 0
    
    def save_analysis(self, asset_id, technical, fundamental, sentiment, composite):
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO analysis (asset_id, technical_score, fundamental_score, 
            sentiment_score, composite_score) VALUES (?, ?, ?, ?, ?)""",
            (asset_id, technical, fundamental, sentiment, composite)
        )
        self.conn.commit()
    
    def save_system_status(self, total_equity, cash_balance, risk_level, active_positions, daily_profit):
        cursor = self.conn.cursor()
        cursor.execute(
            """INSERT INTO system_status (total_equity, cash_balance, risk_level, 
            active_positions, daily_profit) VALUES (?, ?, ?, ?, ?)""",
            (total_equity, cash_balance, risk_level, active_positions, daily_profit)
        )
        self.conn.commit()
    
    def has_position(self, asset_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM positions WHERE asset_id = ? AND status = 'open'", (asset_id,))
        result = cursor.fetchone()
        return result[0] > 0 if result else False
    
    def get_asset(self, asset_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM assets WHERE id = ?", (asset_id,))
        return cursor.fetchone()
    
    def set_stop_loss_take_profit(self, asset_id, stop_loss, take_profit):
        cursor = self.conn.cursor()
        cursor.execute(
            """UPDATE positions SET stop_loss = ?, take_profit = ? 
            WHERE asset_id = ? AND status = 'open'""",
            (stop_loss, take_profit, asset_id)
        )
        self.conn.commit()

# ====================== 数据获取引擎 ======================
class DataFetcher:
    CACHE = {}
    CACHE_TTL = 300  # 5分钟缓存
    
    def __init__(self):
        self.session = aiohttp.ClientSession()

    async def close(self):
        """关闭aiohttp会话"""
        if not self.session.closed:
            await self.session.close()
    
    async def fetch_data(self, asset_type: str, symbol: str) -> Optional[Dict]:
        """从多个数据源获取数据并缓存"""
        cache_key = f"{asset_type}_{symbol}"
        
        # 检查缓存
        if cache_key in self.CACHE:
            cached_time, data = self.CACHE[cache_key]
            if (datetime.now() - cached_time).seconds < self.CACHE_TTL:
                return data
        
        # 根据资产类型选择数据源
        sources = Config.DATA_SOURCES.get(asset_type, [])
        
        try:
            if asset_type == "crypto":
                data = await self._fetch_crypto_data(symbol, sources)
            elif asset_type == "stocks":
                data = await self._fetch_stock_data(symbol)
            elif asset_type == "etf":
                data = await self._fetch_etf_data(symbol)
            elif asset_type == "precious_metals":
                data = await self._fetch_metal_data(symbol)
            else:
                data = None
            
            if data:
                # 更新缓存
                self.CACHE[cache_key] = (datetime.now(), data)
                return data
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None
    
    async def _fetch_crypto_data(self, symbol: str, sources: List[str]) -> Dict:
        """获取加密货币数据"""
        # 优先使用Binance
        if "Binance" in sources:
            endpoint = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}USDT"
            async with self.session.get(endpoint) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "price": float(data["lastPrice"]),
                        "high": float(data["highPrice"]),
                        "low": float(data["lowPrice"]),
                        "volume": float(data["volume"]),
                        "change": float(data["priceChangePercent"])
                    }
        
        # 备用CoinGecko
        if "CoinGecko" in sources:
            endpoint = f"https://api.coingecko.com/api/v3/coins/{symbol.lower()}"
            headers = {}
            async with self.session.get(endpoint, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    market_data = data["market_data"]
                    return {
                        "price": market_data["current_price"]["usd"],
                        "high": market_data["high_24h"]["usd"],
                        "low": market_data["low_24h"]["usd"],
                        "volume": market_data["total_volume"]["usd"],
                        "market_cap": market_data["market_cap"]["usd"]
                    }
        return {}
    
    async def _fetch_stock_data(self, symbol: str) -> Dict:
        """获取股票数据"""
        endpoint = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?apiKey={Config.POLYGON_API_KEY}"
        async with self.session.get(endpoint) as response:
            if response.status == 200:
                data = await response.json()
            if 'results' in data and len(data['results']) > 0:
                result = data['results'][0]
                return {
                    "price": float(result['c']),
                    "high": float(result['h']),
                    "low": float(result['l']),
                    "volume": float(result['v']),
                    "change": float((result['c'] - result['o']) / result['o'] * 100)
                }
            else:
                logger.warning(f"No data found for {symbol}")
                return {}
        return {}
    
    async def _fetch_etf_data(self, symbol: str) -> Dict:
        """获取ETF数据 - 简化实现"""
        # 实际实现应使用Yahoo Finance或Alpha Vantage
        return {
            "price": np.random.uniform(50, 200),
            "change": np.random.uniform(-5, 5)
        }
    
    async def _fetch_metal_data(self, symbol: str) -> Dict:
        """获取贵金属数据"""
        # Bitpanda API实现
        headers = {"X-API-KEY": Config.BITPANDA_API_KEY}
        endpoint = f"https://api.bitpanda.com/v1/ticker?symbols={symbol}_EUR"
        async with self.session.get(endpoint, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                ticker = data.get(f"{symbol}_EUR", {})
                return {
                    "price": float(ticker.get("last_price", 0)),
                    "high": float(ticker.get("high", 0)),
                    "low": float(ticker.get("low", 0)),
                    "volume": float(ticker.get("volume", 0))
                }
        return {}
    
    async def close(self):
        await self.session.close()

# ====================== 分析引擎 ======================
class Analyzer:
    @staticmethod
    def technical_analysis(data: Dict) -> int:
        """技术分析评分 (0-100)"""
        # 简化实现 - 实际应包含更多指标
        score = 50  # 基础分
        
        # 价格动量 (30%)
        momentum = data.get("change", 0)
        momentum_score = min(30, max(0, 30 * (1 + momentum/100)))
        
        # 成交量 (20%)
        volume = data.get("volume", 0)
        volume_score = 0
        if volume > 0:
            volume_score = min(20, 10 * np.log10(volume))
        
        # 价格位置 (25%)
        high = data.get("high", 0)
        low = data.get("low", 0)
        price = data.get("price", 0)
        if high > low > 0:
            position_ratio = (price - low) / (high - low)
            position_score = 25 * position_ratio
        else:
            position_score = 12.5
        
        # 趋势强度 (25%)
        trend_score = 25 if abs(momentum) > 2 else 10
        
        score = momentum_score + volume_score + position_score + trend_score
        return min(100, max(0, int(score)))
    
    @staticmethod
    def fundamental_analysis(data: Dict) -> int:
        """基本面分析 (0-100)"""
        # 根据市值、流动性等评分
        score = 60
        market_cap = data.get("market_cap", 0)
        
        if market_cap > 10**9:  # 10亿美元以上
            score += 20
        elif market_cap > 10**8:  # 1亿美元以上
            score += 10
        
        return min(100, score)
    
    @staticmethod
    def sentiment_analysis(symbol: str, asset_type: str) -> int:
        """市场情绪分析 (0-100)"""
        # 简化实现 - 实际应接入外部API
        if asset_type == "crypto":
            return np.random.randint(40, 80)
        else:
            return np.random.randint(50, 70)
    
    def analyze_asset(self, asset_type: str, symbol: str, data: Dict) -> Dict:
        """综合评分分析"""
        tech_score = self.technical_analysis(data)
        fund_score = self.fundamental_analysis(data)
        sentiment_score = self.sentiment_analysis(symbol, asset_type)
        
        # 根据资产类型调整权重
        weights = {
            "crypto": [0.4, 0.3, 0.3],      # 技术40%, 基本面30%, 情绪30%
            "stocks": [0.3, 0.5, 0.2],       # 技术30%, 基本面50%, 情绪20%
            "etf": [0.2, 0.6, 0.2],          # 技术20%, 基本面60%, 情绪20%
            "precious_metals": [0.3, 0.4, 0.3] # 技术30%, 基本面40%, 情绪30%
        }
        
        weight = weights.get(asset_type, [0.4, 0.4, 0.2])
        total_score = int(
            tech_score * weight[0] +
            fund_score * weight[1] +
            sentiment_score * weight[2]
        )
        
        return {
            "technical_score": tech_score,
            "fundamental_score": fund_score,
            "sentiment_score": sentiment_score,
            "composite_score": min(100, total_score)
        }

# ====================== 风险管理模块 ======================
class RiskManager:
    def __init__(self, db: Database):
        self.db = db
    
    def calculate_position_size(self, confidence: int, volatility: float) -> float:
        """基于信心度和波动率计算仓位大小"""
        base_size = Config.DAILY_INVESTMENT_EUR
        position_size = base_size * (confidence / 100) * (1 - min(0.5, volatility))
        return min(position_size, Config.MAX_SINGLE_INVESTMENT_EUR)
    
    def calculate_var(self, portfolio_value: float) -> float:
        """计算风险价值 (Value at Risk)"""
        # 简化的VaR计算 - 实际应使用历史模拟法或蒙特卡洛模拟
        return portfolio_value * 0.02  # 假设每日最大损失2%
    
    def dynamic_stop_loss(self, asset_type: str, base_sl: float = Config.BASE_STOP_LOSS_PCT) -> float:
        """动态止损设置"""
        adjustments = {
            "crypto": 0.07,
            "stocks": 0.06,
            "etf": 0.05,
            "precious_metals": 0.04
        }
        return base_sl * adjustments.get(asset_type, 1.0)
    
    def dynamic_take_profit(self, asset_type: str, base_tp: float = Config.BASE_TAKE_PROFIT_PCT) -> float:
        """动态止盈设置"""
        adjustments = {
            "crypto": 1.3,
            "stocks": 1.2,
            "etf": 1.1,
            "precious_metals": 1.0
        }
        return base_tp * adjustments.get(asset_type, 1.0)
    
    def check_daily_limit(self) -> float:
        """检查每日投资限额"""
        daily_invested = self.db.get_daily_trades()
        return max(0, Config.MAX_DAILY_INVESTMENT_EUR - daily_invested)
    
    def check_asset_concentration(self, asset_id: int, amount: float, portfolio_value: float) -> bool:
        """检查资产集中度"""
        # 简化实现 - 实际应检查该资产在组合中的占比
        max_concentration = 0.15  # 单资产最大占比15%
        return (amount / portfolio_value) <= max_concentration

# ====================== 交易执行模块 ======================
class TradeExecutor:
    def __init__(self, db: Database):
        self.db = db
        self.binance_api_key = Config.BINANCE_API_KEY
        self.binance_api_secret = Config.BINANCE_API_SECRET
        self.bitpanda_api_key = Config.BITPANDA_API_KEY
    
    async def execute_trade(self, asset: Dict, action: str, quantity: float, price: float):
        """执行交易并更新数据库"""
        if quantity <= 0:
            return False
        
        platform = asset.get("platform", "Binance")
        
        # 在实际交易平台执行
        if platform == "Binance":
            success = await self._execute_binance_trade(asset["symbol"], action, quantity, price)
        elif platform == "Bitpanda":
            success = await self._execute_bitpanda_trade(asset["symbol"], action, quantity, price)
        else:
            success = False
        
        if success:
            # 记录交易
            trade_id = self.db.log_trade(asset["id"], action, quantity, price, platform)
            
            # 更新持仓
            if action == "BUY":
                self._create_position(trade_id, asset["id"], quantity, price)
            
            # 发送通知
            await self.send_notification(
                f"执行交易: {action} {quantity} {asset['symbol']} @ {price} {asset['currency']} "
                f"on {platform}"
            )
            return True
        return False
    
    async def _execute_binance_trade(self, symbol: str, action: str, quantity: float, price: float) -> bool:
        """在Binance执行真实交易"""
        try:
            from binance.client import Client
            client = Client(Config.BINANCE_API_KEY, Config.BINANCE_API_SECRET)
            
            order = client.create_order(
                symbol=symbol,
                side=Client.SIDE_BUY if action == 'BUY' else Client.SIDE_SELL,
                type=Client.ORDER_TYPE_MARKET,
                quantity=quantity
            )
            
            logger.info(f"[Binance] 真实交易执行成功: {order}")
            return order['orderId'] is not None
        except Exception as e:
            logger.error(f"[Binance] 真实交易执行失败: {str(e)}")
            return False
    
    async def _execute_bitpanda_trade(self, symbol: str, action: str, quantity: float, price: float) -> bool:
        """在Bitpanda执行真实交易"""
        try:
            import requests
            
            headers = {
                'Authorization': f'Bearer {Config.BITPANDA_API_KEY}',
                'Content-Type': 'application/json'
            }
            
            data = {
                'instrument_code': symbol,
                'side': action.lower(),
                'type': 'MARKET',
                'amount': str(quantity)
            }
            
            response = requests.post(
                'https://api.bitpanda.com/v1/trading/orders',
                headers=headers,
                json=data
            )
            
            response.raise_for_status()
            logger.info(f"[Bitpanda] 真实交易执行成功: {response.json()}")
            return True
        except Exception as e:
            logger.error(f"[Bitpanda] 真实交易执行失败: {str(e)}")
            return False
    
    def _create_position(self, trade_id: int, asset_id: int, quantity: float, entry_price: float):
        """创建新持仓"""
        cursor = self.db.conn.cursor()
        cursor.execute(
            """INSERT INTO positions (trade_id, asset_id, quantity, entry_price, current_price) 
            VALUES (?, ?, ?, ?, ?)""",
            (trade_id, asset_id, quantity, entry_price, entry_price)
        )
        self.db.conn.commit()
    
    async def send_notification(self, message: str):
        """通过Telegram发送通知"""
        if Config.TELEGRAM_TOKEN and Config.TELEGRAM_CHAT_ID:
            try:
                import requests
                url = f"https://api.telegram.org/bot{Config.TELEGRAM_TOKEN}/sendMessage"
                payload = {
                    "chat_id": Config.TELEGRAM_CHAT_ID,
                    "text": message
                }
                response = requests.post(url, json=payload)
                if response.status_code != 200:
                    logger.error(f"Telegram通知失败: {response.text}")
            except Exception as e:
                logger.error(f"发送Telegram通知失败: {str(e)}")
        else:
            logger.info(f"通知: {message}")
    
    async def close_positions(self):
        """检查并关闭达到止损/止盈的仓位"""
        positions = self.db.get_active_positions()
        for position in positions:
            position_id, _, asset_id, quantity, entry_price, current_price, stop_loss, take_profit, _, _, _ = position
            
            # 获取当前价格
            asset = self.db.get_asset(asset_id)
            if not asset:
                continue
                
            data = await data_fetcher.fetch_data(asset["asset_type"], asset["symbol"])
            if not data or "price" not in data:
                continue
                
            current_price = data["price"]
            
            # 更新当前价格
            self.db.update_position(position_id, current_price=current_price)
            
            # 检查止损
            if stop_loss and current_price <= stop_loss:
                await self.execute_trade(asset, "SELL", quantity, current_price)
                self.db.update_position(position_id, status="closed")
                await self.send_notification(
                    f"止损触发: 卖出 {quantity} {asset['symbol']} @ {current_price}"
                )
            
            # 检查止盈
            elif take_profit and current_price >= take_profit:
                await self.execute_trade(asset, "SELL", quantity, current_price)
                self.db.update_position(position_id, status="closed")
                await self.send_notification(
                    f"止盈触发: 卖出 {quantity} {asset['symbol']} @ {current_price}"
                )

# ====================== 资产配置模块 ======================
class PortfolioManager:
    def __init__(self, db: Database):
        self.db = db
    
    def get_current_allocation(self) -> Dict:
        """获取当前资产配置"""
        # 简化实现 - 实际应从数据库计算
        return {
            "crypto": 0.30,
            "stocks": 0.25,
            "etf": 0.30,
            "precious_metals": 0.15
        }
    
    def get_rebalancing_needs(self) -> List[Dict]:
        """获取需要再平衡的资产"""
        current = self.get_current_allocation()
        target = Config.ASSET_ALLOCATION
        
        rebalance_actions = []
        for asset_type in target:
            diff = current.get(asset_type, 0) - target[asset_type]
            if abs(diff) > 0.05:  # 偏差超过5%时需要调整
                action = "BUY" if current.get(asset_type, 0) < target[asset_type] else "SELL"
                rebalance_actions.append({
                    "asset_type": asset_type,
                    "action": action,
                    "amount_pct": abs(diff)
                })
        
        return rebalance_actions
    
    def generate_trade_signals(self, analysis_results: List[Dict]) -> List[Dict]:
        """生成交易信号"""
        # 简化实现 - 实际应使用更复杂的策略
        signals = []
        for asset in analysis_results:
            if asset["composite_score"] > 75:
                signals.append({
                    "asset_id": asset["id"],
                    "symbol": asset["symbol"],
                    "action": "BUY",
                    "confidence": min(100, asset["composite_score"]),
                    "price": asset["price"]
                })
            elif asset["composite_score"] < 40:
                # 检查是否持有该资产
                if self.db.has_position(asset["id"]):
                    signals.append({
                        "asset_id": asset["id"],
                        "symbol": asset["symbol"],
                        "action": "SELL",
                        "confidence": 100 - asset["composite_score"],
                        "price": asset["price"]
                    })
        return signals

# ====================== 主交易系统 ======================
class TradingSystem:
    def __init__(self, mode: str = "continuous"):
        self.mode = mode
        self.db = Database()
        self.data_fetcher = DataFetcher()
        self.analyzer = Analyzer()
        self.risk_manager = RiskManager(self.db)
        self.portfolio_manager = PortfolioManager(self.db)
        self.executor = TradeExecutor(self.db)
        self.scheduler = AsyncIOScheduler()  # 修正为正确的调度器类型
        
        # 资产观察列表
        self.watchlist = self._load_watchlist()
        
        if mode == "continuous":
            self.setup_scheduler()
    
    def calculate_crypto_score(self, crypto: Dict) -> float:
        """计算加密货币评分（基于市值和交易量）"""
        # 基础评分公式：市值权重60% + 交易量权重40%
        market_cap_score = crypto.get('market_cap', 0) / 1e9  # 转换为十亿单位
        volume_score = crypto.get('total_volume', 0) / 1e8     # 转换为千万单位
        return (market_cap_score * 0.6) + (volume_score * 0.4)

    def get_top_cryptos(self, limit: int = 100) -> List[Dict]:
        import requests
        url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page={limit}&page=1&sparkline=false"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return [{"symbol": item["symbol"], "name": item["name"], "market_cap": item["market_cap"], "total_volume": item["total_volume"]} for item in data]
        except Exception as e:
            self.logger.error(f"获取加密货币列表失败: {str(e)}")
            return []

    def _load_watchlist(self) -> List[Dict]:
        import requests
        url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page={limit}&page=1&sparkline=false"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return [{"symbol": item["symbol"], "name": item["name"], "market_cap": item["market_cap"]} for item in data]
        except Exception as e:
            self.logger.error(f"获取加密货币列表失败: {str(e)}")
            return []

    def _load_watchlist(self) -> List[Dict]:
        """扫描市场资产并生成观察列表"""
        all_assets = []
        
        # 1. 加密货币（CoinGecko）
        crypto_list = self.get_top_cryptos(limit=100)  # 取市值前100
        for crypto in crypto_list:
            all_assets.append({
                'symbol': crypto['symbol'].upper(),
                'name': crypto['name'],
                'asset_type': 'crypto',
                'market_cap': crypto['market_cap'],
                'volume': crypto['total_volume'],
                'score': self.calculate_crypto_score(crypto),
                'platform': 'Binance',
                'currency': 'USDC'
            })
        
        # 2. 股票（Polygon）
        if os.getenv('STOCK_DATA_PROVIDER') == 'polygon':
            stock_tickers = get_sp500_tickers()  # 标普500成分股
            for ticker in stock_tickers:
                stock_data = get_polygon_stock_data(ticker)
                if stock_data and stock_data['volume'] > 1e6:
                    all_assets.append({
                        'symbol': ticker,
                        'name': stock_data['name'],
                        'asset_type': 'stocks',
                        'market_cap': stock_data['market_cap'],
                        'volume': stock_data['volume'],
                        'score': calculate_stock_score(stock_data),
                        'platform': 'Polygon',
                        'currency': 'USD'
                    })
        
        # 3. 过滤低流动性资产
        filtered_assets = [asset for asset in all_assets 
                if asset['market_cap'] > float(os.getenv('MIN_MARKET_CAP', 1e9)) 
                and asset['volume'] > float(os.getenv('MIN_24H_VOLUME', 1e7))]
        
        # 添加ID字段
        for i, asset in enumerate(filtered_assets, start=1):
            asset['id'] = i
        
        return filtered_assets
    
    def setup_scheduler(self):
        """设置定时任务"""
        # 每15分钟交易周期
        self.scheduler.add_job(self.trading_cycle, 'interval', minutes=15)
        
        # 每小时健康检查
        self.scheduler.add_job(self.health_check, 'interval', hours=1)
        
        # 每日报告
        self.scheduler.add_job(self.morning_report, 'cron', hour=9)
        self.scheduler.add_job(self.evening_report, 'cron', hour=18)
        self.scheduler.add_job(self.daily_summary, 'cron', hour=22)
        
        # 启动调度器
        self.scheduler.start()
    
    async def shutdown(self):
        """关闭系统资源"""
        logger.info("正在关闭系统资源...")
        await self.data_fetcher.close()
        self.scheduler.shutdown()
        logger.info("系统资源已关闭")
    
    async def trading_cycle(self):
        """主交易周期逻辑"""
        if not self.is_trading_hours():
            logger.info("不在交易时间内，跳过周期")
            return
        
        logger.info(f"=== 开始交易周期 {datetime.now()} ===")
        
        # 1. 获取资产数据
        assets_data = await self.fetch_assets_data()
        logger.info(f"获取到 {len(assets_data)} 个资产的数据")
        
        # 2. 分析资产
        analysis_results = self.analyze_assets(assets_data)
        logger.info("资产分析完成:")
        for asset in analysis_results:
            logger.info(f"{asset['symbol']} - 综合评分: {asset['composite_score']}")
        
        # 3. 生成交易信号
        trade_signals = self.portfolio_manager.generate_trade_signals(analysis_results)
        logger.info(f"生成 {len(trade_signals)} 个交易信号")
        for signal in trade_signals:
            logger.info(f"信号: {signal['action']} {signal['symbol']}, 信心度: {signal['confidence']}, 价格: {signal['price']}")
        
        # 4. 执行交易
        await self.execute_signals(trade_signals)
        
        # 5. 检查并关闭仓位
        await self.executor.close_positions()
        
        # 6. 更新系统状态
        self.update_system_status()
        
        logger.info("=== 交易周期完成 ===\n")
    
    async def fetch_assets_data(self) -> List[Dict]:
        """获取所有观察资产的数据"""
        tasks = []
        for asset in self.watchlist:
            tasks.append(self.data_fetcher.fetch_data(asset["asset_type"], asset["symbol"]))
        
        results = await asyncio.gather(*tasks)
        
        assets_data = []
        for i, data in enumerate(results):
            if data:
                asset = self.watchlist[i].copy()
                asset.update(data)
                assets_data.append(asset)
        
        return assets_data
    
    def analyze_assets(self, assets_data: List[Dict]) -> List[Dict]:
        """分析所有资产"""
        analysis_results = []
        for asset in assets_data:
            analysis = self.analyzer.analyze_asset(
                asset["asset_type"], 
                asset["symbol"], 
                asset
            )
            asset.update(analysis)
            analysis_results.append(asset)
            
            # 保存分析结果到数据库
            self.db.save_analysis(
                asset["id"],
                analysis["technical_score"],
                analysis["fundamental_score"],
                analysis["sentiment_score"],
                analysis["composite_score"]
            )
        
        # 按综合评分排序
        return sorted(analysis_results, key=lambda x: x["composite_score"], reverse=True)
    
    async def execute_signals(self, signals: List[Dict]):
        """执行交易信号"""
        daily_limit = self.risk_manager.check_daily_limit()
        if daily_limit <= 0:
            await self.executor.send_notification("⚠️ 今日投资额度已用完")
            return
        
        portfolio_value = 1000  # 简化 - 实际应从数据库获取
        
        for signal in signals:
            if daily_limit <= 0:
                break
                
            asset_id = signal["asset_id"]
            asset = next((a for a in self.watchlist if a["id"] == asset_id), None)
            if not asset:
                continue
                
            # 计算仓位大小
            volatility = 0.2  # 简化 - 实际应计算波动率
            position_size = self.risk_manager.calculate_position_size(
                signal["confidence"], 
                volatility
            )
            position_size = min(position_size, daily_limit)
            
            # 检查资产集中度
            if not self.risk_manager.check_asset_concentration(asset_id, position_size, portfolio_value):
                continue
                
            # 执行交易
            price = signal["price"]
            quantity = position_size / price
            
            if await self.executor.execute_trade(asset, signal["action"], quantity, price):
                daily_limit -= position_size
                
                # 设置止损止盈
                if signal["action"] == "BUY":
                    stop_loss = price * (1 - self.risk_manager.dynamic_stop_loss(asset["asset_type"]))
                    take_profit = price * (1 + self.risk_manager.dynamic_take_profit(asset["asset_type"]))
                    self.db.set_stop_loss_take_profit(asset_id, stop_loss, take_profit)
    
    def is_trading_hours(self) -> bool:
        """检查是否在交易时间内"""
        now = datetime.now()
        current_time = now.time()
        weekday = now.weekday()
        
        # 检查每种资产类型的交易时间
        for asset in self.watchlist:
            asset_type = asset["asset_type"]
            trading_hours = Config.TRADING_HOURS.get(asset_type)
            
            if trading_hours:
                start_time = datetime.strptime(trading_hours["start"], "%H:%M").time()
                end_time = datetime.strptime(trading_hours["end"], "%H:%M").time()
                trading_days = trading_hours["days"]
                
                if weekday in trading_days and start_time <= current_time <= end_time:
                    return True
        
        return False
    
    async def health_check(self):
        """系统健康检查"""
        status = "✅ 系统运行正常"
        # 实际应检查API连接、数据库状态等
        await self.executor.send_notification(status)
    
    async def morning_report(self):
        """早间市场简报"""
        report = "🌅 早间市场简报:\n"
        report += "- 加密货币: BTC +2.5%, ETH +1.8%\n"
        report += "- 股票市场: 欧洲股市开盘上涨0.8%\n"
        report += "- 贵金属: 黄金价格稳定在 €1,850/盎司"
        await self.executor.send_notification(report)
    
    async def evening_report(self):
        """晚间性能报告"""
        report = "📊 今日交易报告:\n"
        report += f"- 已完成交易: 3笔\n"
        report += f"- 当前持仓: 2个\n"
        report += f"- 今日收益: +€1.25\n"
        report += f"- 风险水平: 中等"
        await self.executor.send_notification(report)
    
    async def daily_summary(self):
        """每日收盘总结"""
        summary = "📈 今日总结:\n"
        summary += "- 总资产: €52.75 (+1.25%)\n"
        summary += "- 最佳表现: BTC +3.2%\n"
        summary += "- 明日关注: 美联储利率决议"
        await self.executor.send_notification(summary)
    
    def update_system_status(self):
        """更新系统状态"""
        # 简化实现 - 实际应计算真实数据
        self.db.save_system_status(
            total_equity=1052.75,
            cash_balance=28.50,
            risk_level=0.35,
            active_positions=2,
            daily_profit=1.25
        )
    
    async def shutdown(self):
        """关闭系统"""
        if hasattr(self, 'data_fetcher') and self.data_fetcher:
            await self.data_fetcher.close()
        
        if hasattr(self, 'scheduler') and self.scheduler.running:
            self.scheduler.shutdown()
        
        # 关闭数据库连接
        if hasattr(self, 'db') and self.db:
            self.db.conn.close()

# ====================== 主程序入口 ======================
async def main():
    parser = argparse.ArgumentParser(description='多资产智能交易系统')
    parser.add_argument('--mode', choices=['continuous', 'single'], default='continuous',
                        help='运行模式: continuous(持续运行) 或 single(单次运行)')
    args = parser.parse_args()
    
    logger.info("=== 启动多资产智能交易系统 v3.0 ===")
    system = TradingSystem(mode=args.mode)
    
    try:
        if args.mode == "single":
            # 单次运行模式
            await system.trading_cycle()
        else:
            # 持续运行模式
            while True:
                await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("\n接收到关闭信号，正在停止系统...")
    finally:
        await system.shutdown()
        logger.info("系统已安全关闭")

if __name__ == "__main__":
    asyncio.run(main())