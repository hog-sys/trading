@echo off
title 多资产智能交易系统
echo 正在启动交易系统...

:: 安装依赖包
pip install -r requirements.txt

:: 初始化系统数据库
python db_init.py

:: 启动主交易程序
python trading_system.py --mode=continuous

pause