@echo off
cd /d C:\Users\juddu\Downloads\PAM\MDMH4H
set FLASK_ENV=development
set FULCRUM_PORT=5087
set FULCRUM_APP_BASE_URL=http://127.0.0.1:5087
C:\Users\juddu\AppData\Local\Programs\Python\Python312\python.exe .\run_fulcrum_alpha.py
