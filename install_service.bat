@echo off
echo Installing Trading System as Windows Service...

:: Create service installation script
echo import win32serviceutil > trading_service.py
echo import win32service >> trading_service.py
echo import win32event >> trading_service.py
echo import servicemanager >> trading_service.py
echo import subprocess >> trading_service.py
echo. >> trading_service.py
echo class TradingService(win32serviceutil.ServiceFramework): >> trading_service.py
echo     _svc_name_ = "TradingSystem" >> trading_service.py
echo     _svc_display_name_ = "Multi-Asset Trading System" >> trading_service.py
echo. >> trading_service.py
echo     def __init__(self, args): >> trading_service.py
echo         super().__init__(args) >> trading_service.py
echo         self.hWaitStop = win32event.CreateEvent(None, 0, 0, None) >> trading_service.py
echo. >> trading_service.py
echo     def SvcStop(self): >> trading_service.py
echo         self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING) >> trading_service.py
echo         win32event.SetEvent(self.hWaitStop) >> trading_service.py
echo. >> trading_service.py
echo     def SvcDoExecute(self): >> trading_service.py
echo         servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE, >> trading_service.py
echo                              servicemanager.PYS_SERVICE_STARTED, >> trading_service.py
echo                              (self._svc_name_, '')) >> trading_service.py
echo         subprocess.call(["python", "trading_system.py", "--mode=continuous"]) >> trading_service.py
echo. >> trading_service.py
echo if __name__ == '__main__': >> trading_service.py
echo     win32serviceutil.HandleCommandLine(TradingService) >> trading_service.py

:: 安装服务
pip install pywin32
python trading_service.py install
net start TradingSystem

echo Service installed successfully!
pause