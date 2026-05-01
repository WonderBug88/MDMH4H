@echo off
cd /d C:\Users\juddu\Downloads\PAM\MDMH4H
set FULCRUM_ENV_PATH=C:\Users\juddu\Downloads\PAM\fulcrum.alpha.env
set ENABLE_SCHEDULER=0
set FULCRUM_HOST=127.0.0.1
set FULCRUM_PORT=5087
set FLASK_ENV=development
C:\Users\juddu\AppData\Local\Programs\Python\Python312\python.exe run_fulcrum_alpha.py >> C:\Users\juddu\Downloads\PAM\MDMH4H\tmp_fulcrum_5087_cmd.log 2>&1