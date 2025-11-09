@echo off
echo ========================================
echo ลบ Python Cache และรัน Streamlit
echo ========================================
echo.

echo กำลังลบ __pycache__...
if exist __pycache__ rmdir /s /q __pycache__
echo ✅ ลบ cache แล้ว
echo.

echo กำลังรัน Streamlit...
streamlit run bot_data_app.py --server.runOnSave=true
pause

