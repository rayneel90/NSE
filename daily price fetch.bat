@echo off
cmd /k "venv\Scripts\activate & python pyscript\fetch_daily_data_alphavantage.py & python pyscript\fetch_historical_price_alphavantage.py"