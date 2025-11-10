import streamlit as st

# ตั้งค่า page config เป็นคำสั่ง Streamlit แรกสุด (ต้องอยู่ก่อนคำสั่ง Streamlit ใดๆ)
st.set_page_config(
    page_title="Bank PDF to Excel Converter",
    page_icon="🏦",
    layout="wide"
)

import pandas as pd
import pdfplumber
from datetime import datetime, timedelta
import io
import re
import zipfile
from typing import Dict, List, Tuple, Optional, Any
import os
import sys
import importlib.util
import importlib.machinery
import importlib
import time
import logging
import asyncio
import subprocess
from concurrent.futures import ThreadPoolExecutor

try:
    from NewPeak import NewPeakBot
except ImportError:
    NewPeakBot = None

# ตั้งค่า logging ก่อน (เพื่อใช้ logger ในการตรวจสอบ config)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import config
try:
    import config
    # ตรวจสอบว่ามี Link_conpany และ Link_receipt หรือไม่
    if not hasattr(config, 'Link_conpany'):
        logger.warning("⚠️ config module ไม่มี Link_conpany attribute")
    else:
        logger.info(f"✅ พบ Link_conpany ใน config: {getattr(config, 'Link_conpany', None)}")
    if not hasattr(config, 'Link_receipt'):
        logger.warning("⚠️ config module ไม่มี Link_receipt attribute")
    else:
        logger.info(f"✅ พบ Link_receipt ใน config: {getattr(config, 'Link_receipt', None)}")
except ImportError:
    config = None
    logger.error("❌ ไม่สามารถ import config ได้")

# เก็บ bot instances ไว้ใน module level เพื่อป้องกัน garbage collection
_peakengine_bots = []
_newpeak_bots = []

# แก้ปัญหา asyncio event loop บน Windows ให้รองรับ subprocess (Playwright)
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

# Import DBDDataWarehouseBot จาก bot_data.py
try:
    # ลบ cache เก่าเพื่อให้แน่ใจว่าโหลดโค้ดใหม่
    modules_to_remove = [key for key in sys.modules.keys() if 'bot_data' in key.lower()]
    for module_name in modules_to_remove:
        try:
            del sys.modules[module_name]
        except:
            pass
    
    # ลบ __pycache__ ด้วย
    import shutil
    import inspect
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cache_dir = os.path.join(current_dir, '__pycache__')
    if os.path.exists(cache_dir):
        try:
            shutil.rmtree(cache_dir, ignore_errors=True)
        except:
            pass
    
    # Import จากไฟล์แยก
    bot_data_path = os.path.join(current_dir, 'bot_data.py')
    
    if os.path.exists(bot_data_path):
        # ใช้ unique module name เพื่อบังคับให้โหลดใหม่ทุกครั้ง
        module_name = f"bot_data_module_{int(time.time() * 1000)}"
        spec = importlib.util.spec_from_file_location(module_name, bot_data_path)
        bot_data_module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = bot_data_module
        spec.loader.exec_module(bot_data_module)
        
        # ตรวจสอบว่า class มี use_browser parameter หรือไม่
        sig = inspect.signature(bot_data_module.DBDDataWarehouseBot.__init__)
        params = list(sig.parameters.keys())
        
        if 'use_browser' not in params:
            raise AttributeError(f"bot_data.py ไม่มี use_browser parameter ใน __init__ (พบ parameters: {params})")
        
        DBDDataWarehouseBot = bot_data_module.DBDDataWarehouseBot
        create_dbd_summary_table = bot_data_module.create_dbd_summary_table
        
        logger.info(f"✅ โหลด bot_data.py สำเร็จ (parameters: {params})")
    else:
        # Fallback
        logger.warning(f"ไม่พบไฟล์ bot_data.py ที่ {bot_data_path}")
        from bot_data import DBDDataWarehouseBot, create_dbd_summary_table
        
        # ตรวจสอบอีกครั้ง
        sig = inspect.signature(DBDDataWarehouseBot.__init__)
        params = list(sig.parameters.keys())
        if 'use_browser' not in params:
            raise AttributeError(f"bot_data module ไม่มี use_browser parameter (พบ: {params})")
        
except Exception as e:
    error_msg = str(e)
    logger.error(f"ไม่สามารถโหลด bot_data module ได้: {error_msg}")
    st.error(f"❌ ไม่สามารถโหลด bot_data module ได้: {error_msg}")
    st.error(f"กรุณาตรวจสอบว่าไฟล์ bot_data.py มี use_browser parameter ใน __init__")
    st.error(f"Path ที่ลองหา: {bot_data_path if 'bot_data_path' in locals() else 'ไม่พบ'}")
    
    # Fallback: สร้าง class เก่าเป็น dummy (แต่มี use_browser)
    class DBDDataWarehouseBot:
        def __init__(self, use_browser=False, headless=False):
            st.error(f"ไม่สามารถโหลด bot_data.py ได้: {error_msg}")
            st.stop()
        
        def search_company_info(self, company_name, log_callback=None):
            return {"error": "ไม่สามารถโหลด bot_data module ได้"}
        
        def format_company_info(self, company_info):
            return "ไม่พบข้อมูล"
    
    def create_dbd_summary_table(df):
        return pd.DataFrame()

# ฟังก์ชันสำหรับใช้งานบอทกับ Streamlit
def integrate_with_streamlit(df: pd.DataFrame, company_column: str = 'ชื่อบริษัท/บุคคล',
                              use_browser: bool = False, headless: bool = False) -> pd.DataFrame:
    """ฟังก์ชันสำหรับใช้งานร่วมกับ Streamlit พร้อมแสดงการทำงาน"""
    # สร้าง bot instance พร้อม browser mode
    bot = DBDDataWarehouseBot(use_browser=use_browser, headless=headless)
    
    # สร้างคอลัมน์ใหม่สำหรับข้อมูล DBD
    df['ข้อมูล DBD'] = ""
    df['ชื่อบริษัทจาก DBD'] = ""
    address_column_map = [
        ('ที่อยู่_บ้านเลขที่', 'address_house_no'),
        ('ที่อยู่_หมู่บ้าน', 'address_village'),
        ('ที่อยู่_หมู่ที่', 'address_moo'),
        ('ที่อยู่_ตำบล', 'address_subdistrict'),
        ('ที่อยู่_อำเภอ', 'address_district'),
        ('ที่อยู่_จังหวัด', 'address_province'),
        ('ที่อยู่_รหัสไปรษณีย์', 'address_postal_code')
    ]

    for column_name, _ in address_column_map:
        if column_name not in df.columns:
            df[column_name] = ""
    
    # สร้าง progress bar และ status
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # สร้างสถิติ
    stats_container = st.container()
    with stats_container:
        col1, col2, col3, col4 = st.columns(4)
        success_count = col1.metric("✅ สำเร็จ", "0")
        error_count = col2.metric("❌ ข้อผิดพลาด", "0")
        not_found_count = col3.metric("🔍 ไม่พบข้อมูล", "0")
        total_count = col4.metric("📊 รวม", "0")
    
    # สร้าง container สำหรับแสดงขั้นตอนการทำงาน
    log_container = st.container()
    with log_container:
        st.subheader("📋 ขั้นตอนการทำงานของบอท")
        log_expander = st.expander("🔍 ดูขั้นตอนการทำงานแบบละเอียด", expanded=False)
        log_messages = []
        log_placeholder = log_expander.empty()
    
    # ดึงข้อมูลสำหรับแต่ละบริษัท
    total_companies = len(df[df[company_column].notna() & (df[company_column] != '')])
    processed_count = 0
    success_stats = 0
    error_stats = 0
    not_found_stats = 0
    
    def log_callback(message, status="info"):
        """Callback สำหรับแสดง log"""
        log_messages.append({
            "message": message,
            "status": status,
            "time": datetime.now().strftime("%H:%M:%S")
        })
        
        # แสดง log ใน expander
        log_text = ""
        for log in log_messages[-50:]:  # แสดงล่าสุด 50 รายการ
            icon = {
                "info": "ℹ️",
                "success": "✅",
                "warning": "⚠️",
                "error": "❌"
            }.get(log["status"], "📝")
            
            log_text += f"[{log['time']}] {icon} {log['message']}\n"
        
        log_placeholder.code(log_text, language=None)
    
    for index, row in df.iterrows():
        company_name = row[company_column]
        
        if pd.isna(company_name) or not str(company_name).strip():
            continue
        
        # อัปเดต status
        processed_count += 1
        progress = processed_count / total_companies
        progress_bar.progress(progress)
        
        status_text.text(f"กำลังประมวลผล {processed_count}/{total_companies}: {company_name}")
        
        # ค้นหาข้อมูลบริษัท (พร้อม log callback)
        company_info = bot.search_company_info(str(company_name), log_callback=log_callback)
        
        # จัดรูปแบบข้อมูลสำหรับใส่ในคอลัมน์
        formatted_info = bot.format_company_info(company_info)
        df.at[index, 'ข้อมูล DBD'] = formatted_info
        if isinstance(company_info, dict):
            if company_info.get("company_name"):
                df.at[index, 'ชื่อบริษัทจาก DBD'] = company_info.get("company_name")

            for column_name, key_name in address_column_map:
                if key_name in company_info:
                    df.at[index, column_name] = company_info.get(key_name, "")
        
        # อัปเดตสถิติ
        if "error" in company_info:
            error_stats += 1
            st.warning(f"⚠️ {company_name}: {company_info['error']}")
        elif formatted_info == "ไม่พบข้อมูล":
            not_found_stats += 1
            st.info(f"🔍 {company_name}: ไม่พบข้อมูล")
        else:
            success_stats += 1
            st.success(f"✅ {company_name}: พบข้อมูล")
        
        # อัปเดต metrics
        success_count.metric("✅ สำเร็จ", str(success_stats))
        error_count.metric("❌ ข้อผิดพลาด", str(error_stats))
        not_found_count.metric("🔍 ไม่พบข้อมูล", str(not_found_stats))
        total_count.metric("📊 รวม", str(processed_count))
        
        # หน่วงเวลาเพื่อไม่ให้โหลดเซิร์ฟเวอร์หนักเกินไป
        time.sleep(0.5)
    
    # แสดงสรุปสุดท้าย
    st.markdown("---")
    st.subheader("📊 สรุปการทำงาน")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("✅ สำเร็จ", success_stats, delta=f"{success_stats/total_companies*100:.1f}%")
    with col2:
        st.metric("❌ ข้อผิดพลาด", error_stats, delta=f"{error_stats/total_companies*100:.1f}%")
    with col3:
        st.metric("🔍 ไม่พบข้อมูล", not_found_stats, delta=f"{not_found_stats/total_companies*100:.1f}%")
    
    # ล้าง progress bar และ status
    progress_bar.empty()
    status_text.empty()
    
    return df

def create_dbd_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """สร้างตารางสรุปข้อมูล DBD"""
    if 'ข้อมูล DBD' not in df.columns:
        return pd.DataFrame()
    
    # สร้างตารางสรุป
    summary_data = []
    
    for index, row in df.iterrows():
        dbd_info = row.get('ข้อมูล DBD', '')
        company_name = row.get('ชื่อบริษัท/บุคคล', '')
        db_company_name = row.get('ชื่อบริษัทจาก DBD', '')
        
        if dbd_info and dbd_info != "ไม่พบข้อมูล" and "ข้อผิดพลาด" not in dbd_info:
            # แยกข้อมูล
            info_parts = dbd_info.split(' | ')
            
            summary_row = {
                'ชื่อบริษัท': company_name,
                'ชื่อบริษัทจาก DBD': db_company_name,
                'เลขทะเบียน': '',
                'ประเภทธุรกิจ': '',
                'สถานะ': '',
                'ทุนจดทะเบียน': '',
                'ที่อยู่': '',
                'ที่อยู่_บ้านเลขที่': row.get('ที่อยู่_บ้านเลขที่', ''),
                'ที่อยู่_หมู่บ้าน': row.get('ที่อยู่_หมู่บ้าน', ''),
                'ที่อยู่_หมู่ที่': row.get('ที่อยู่_หมู่ที่', ''),
                'ที่อยู่_ตำบล': row.get('ที่อยู่_ตำบล', ''),
                'ที่อยู่_อำเภอ': row.get('ที่อยู่_อำเภอ', ''),
                'ที่อยู่_จังหวัด': row.get('ที่อยู่_จังหวัด', ''),
                'ที่อยู่_รหัสไปรษณีย์': row.get('ที่อยู่_รหัสไปรษณีย์', '')
            }
            
            for part in info_parts:
                if 'เลขทะเบียน:' in part:
                    summary_row['เลขทะเบียน'] = part.replace('เลขทะเบียน:', '').strip()
                elif 'ประเภทธุรกิจ:' in part:
                    summary_row['ประเภทธุรกิจ'] = part.replace('ประเภทธุรกิจ:', '').strip()
                elif 'สถานะ:' in part:
                    summary_row['สถานะ'] = part.replace('สถานะ:', '').strip()
                elif 'ทุนจดทะเบียน:' in part:
                    summary_row['ทุนจดทะเบียน'] = part.replace('ทุนจดทะเบียน:', '').strip()
                elif 'ที่อยู่:' in part:
                    summary_row['ที่อยู่'] = part.replace('ที่อยู่:', '').strip()
            
            summary_data.append(summary_row)
    
    return pd.DataFrame(summary_data)


def test_playwright_browser(url: str = "https://datawarehouse.dbd.go.th/index") -> bool:
    """ทดสอบการเปิด Playwright Chromium ผ่านคำสั่ง CLI"""
    try:
        command = [sys.executable, "-m", "playwright", "open", url]
        subprocess.Popen(command)
        return True
    except FileNotFoundError:
        st.error("❌ ไม่พบคำสั่ง playwright CLI โปรดติดตั้งด้วยคำสั่ง `pip install playwright` และ `playwright install chromium`")
    except Exception as e:
        st.error(f"❌ ไม่สามารถเปิด Playwright Browser ได้: {e}")
    return False

def open_peakengine_login() -> bool:
    """เปิดหน้าเว็บ PeakEngine Login และกรอก username/password อัตโนมัติ"""
    try:
        # Reload config module เพื่อให้แน่ใจว่าโหลดค่าล่าสุด
        global config
        if config is not None:
            try:
                import importlib
                importlib.reload(config)
                logger.info("🔄 Reload config module ที่จุดเริ่มต้นฟังก์ชันสำเร็จ")
            except Exception as e:
                logger.warning(f"⚠️ ไม่สามารถ reload config ได้: {e}")
        
        # ตรวจสอบว่ามี config หรือไม่
        if config is None:
            st.error("❌ ไม่พบไฟล์ config.py")
            return False
        
        # Debug: ตรวจสอบ attributes ใน config
        try:
            attrs = [attr for attr in dir(config) if not attr.startswith('_')]
            logger.info(f"🔍 Attributes ใน config (ที่จุดเริ่มต้น): {', '.join(attrs)}")
        except Exception as e:
            logger.warning(f"⚠️ ไม่สามารถตรวจสอบ attributes ได้: {e}")
        
        # ดึงข้อมูลจาก config
        url = getattr(config, 'PEAKENGINE_LOGIN_URL', 'https://secure.peakengine.com/Home/Login')
        username = getattr(config, 'PEAKENGINE_USERNAME', '')
        password = getattr(config, 'PEAKENGINE_PASSWORD', '')
        headless = getattr(config, 'HEADLESS_MODE', False)
        
        # Debug: ตรวจสอบลิงค์
        link_company_check = getattr(config, 'Link_conpany', None)
        link_receipt_check = getattr(config, 'Link_receipt', None)
        logger.info(f"🔍 ตรวจสอบลิงค์ที่จุดเริ่มต้น - Link_conpany: {repr(link_company_check)}, Link_receipt: {repr(link_receipt_check)}")
        
        if not username or not password:
            st.warning("⚠️ กรุณากรอก username และ password ในไฟล์ config.py")
            # เปิดหน้าเว็บโดยไม่กรอกข้อมูล
            command = [sys.executable, "-m", "playwright", "open", url]
            subprocess.Popen(command)
            return True
        
        # ใช้ PeakEngineBot class แทน (จัดการ browser lifecycle ได้ดีกว่า)
        try:
            from peakengine_bot import PeakEngineBot
            
            def run_bot():
                """รัน bot ใน thread แยก"""
                bot = None
                try:
                    # Reload config module เพื่อให้แน่ใจว่าโหลดค่าล่าสุด
                    try:
                        import importlib
                        import config as config_module
                        importlib.reload(config_module)
                        # อัปเดต config reference
                        global config
                        config = config_module
                        logger.info("🔄 Reload config module สำเร็จ")
                    except Exception as e:
                        logger.warning(f"⚠️ ไม่สามารถ reload config ได้: {e}")
                    
                    # สร้าง bot instance
                    bot = PeakEngineBot(use_browser=True, headless=headless)
                    
                    # เก็บ bot instance ไว้ใน module-level list เพื่อป้องกัน garbage collection
                    global _peakengine_bots
                    _peakengine_bots.append(bot)
                    logger.info(f"📝 เก็บ bot instance ไว้ (จำนวนทั้งหมด: {len(_peakengine_bots)})")
                    
                    # Debug: ตรวจสอบ attributes ใน config
                    try:
                        attrs = [attr for attr in dir(config) if not attr.startswith('_')]
                        logger.info(f"🔍 Attributes ใน config: {', '.join(attrs)}")
                    except Exception as e:
                        logger.warning(f"⚠️ ไม่สามารถตรวจสอบ attributes ได้: {e}")
                    
                    # อ่านลิงค์จาก config
                    link_company = getattr(config, 'Link_conpany', None)
                    link_receipt = getattr(config, 'Link_receipt', None)
                    
                    # Debug: ตรวจสอบค่าที่อ่านได้
                    logger.info(f"🔍 link_company = {repr(link_company)}")
                    logger.info(f"🔍 link_receipt = {repr(link_receipt)}")
                    
                    if link_company:
                        logger.info(f"📖 อ่าน Link_conpany จาก config: {link_company}")
                    else:
                        logger.warning("⚠️ ไม่พบ Link_conpany ใน config.py")
                        # ลองอ่านโดยตรง
                        try:
                            if hasattr(config, 'Link_conpany'):
                                link_company = config.Link_conpany
                                logger.info(f"✅ อ่าน Link_conpany โดยตรงได้: {link_company}")
                            else:
                                logger.warning("⚠️ config ไม่มี attribute Link_conpany")
                        except Exception as e:
                            logger.error(f"❌ เกิดข้อผิดพลาดในการอ่าน Link_conpany: {e}")
                    
                    if link_receipt:
                        logger.info(f"📖 อ่าน Link_receipt จาก config: {link_receipt}")
                    else:
                        logger.warning("⚠️ ไม่พบ Link_receipt ใน config.py")
                        # ลองอ่านโดยตรง
                        try:
                            if hasattr(config, 'Link_receipt'):
                                link_receipt = config.Link_receipt
                                logger.info(f"✅ อ่าน Link_receipt โดยตรงได้: {link_receipt}")
                            else:
                                logger.warning("⚠️ config ไม่มี attribute Link_receipt")
                        except Exception as e:
                            logger.error(f"❌ เกิดข้อผิดพลาดในการอ่าน Link_receipt: {e}")
                    
                    # เปิดหน้าเว็บ, กรอกข้อมูล และคลิกปุ่ม Login
                    def log_callback(message, status="info"):
                        logger.info(f"[{status.upper()}] {message}")
                    
                    success = bot.open_login_page_and_fill(username, password, link_company=link_company, link_receipt=link_receipt, log_callback=log_callback)
                    
                    if success:
                        logger.info("✅ เปิดหน้าเว็บ, กรอกข้อมูล, คลิกปุ่ม Login, คลิกปุ่ม PEAK (Deprecated) และ navigate ไปที่ลิงค์สำเร็จ!")
                        logger.info("👀 Browser จะยังเปิดอยู่ - ระบบจะพยายาม login, คลิกปุ่ม PEAK (Deprecated) และ navigate อัตโนมัติ")
                    else:
                        logger.warning("⚠️ ไม่สามารถกรอกข้อมูล, คลิกปุ่ม Login, คลิกปุ่ม PEAK (Deprecated) หรือ navigate ได้ - กรุณาตรวจสอบ log")
                    
                    # ไม่ปิด browser เพื่อให้ผู้ใช้สามารถใช้งานต่อได้
                    # bot.close()  # ไม่เรียก close() เพื่อให้ browser เปิดอยู่
                    # หมายเหตุ: bot instance ถูกเก็บไว้ใน _peakengine_bots เพื่อป้องกัน garbage collection
                    
                except Exception as e:
                    logger.error(f"❌ เกิดข้อผิดพลาด: {e}")
                    logger.error(f"Error details: {e}", exc_info=True)
                    # ถ้าเกิด error และ bot ถูกสร้างแล้ว อาจจะต้องปิด browser
                    # แต่ในกรณีนี้เราจะปล่อยให้ browser เปิดอยู่เพื่อให้ผู้ใช้ตรวจสอบ
            
            # รันใน thread แยก
            executor = ThreadPoolExecutor(max_workers=1)
            executor.submit(run_bot)
            
            return True
            
        except ImportError:
            logger.warning("⚠️ ไม่พบ peakengine_bot.py - ใช้วิธีเปิด browser ธรรมดาแทน")
            # Fallback: เปิด browser ธรรมดา
            command = [sys.executable, "-m", "playwright", "open", url]
            subprocess.Popen(command)
            return True
        
    except FileNotFoundError:
        st.error("❌ ไม่พบคำสั่ง playwright CLI โปรดติดตั้งด้วยคำสั่ง `pip install playwright` และ `playwright install chromium`")
    except ImportError:
        st.error("❌ ไม่พบ playwright library โปรดติดตั้งด้วยคำสั่ง `pip install playwright`")
    except Exception as e:
        st.error(f"❌ ไม่สามารถเปิดหน้าเว็บ PeakEngine ได้: {e}")
        logger.error(f"Error details: {e}", exc_info=True)
    return False


def open_newpeak_login() -> bool:
    """เปิดหน้าเว็บ PEAK Account (ระบบใหม่) และทดสอบการ Login ด้วย NewPeakBot"""
    if NewPeakBot is None:
        st.error("❌ ไม่พบคลาส NewPeakBot (ตรวจสอบว่าไฟล์ NewPeak.py อยู่ในโฟลเดอร์เดียวกัน)")
        logger.error("NewPeakBot ไม่พร้อมใช้งาน - ไม่พบโมดูล NewPeak")
        return False

    if config is None:
        st.error("❌ ไม่พบไฟล์ config.py สำหรับกำหนดการเข้าสู่ระบบ NewPeak")
        return False

    username = getattr(config, "NEWPEAK_USERNAME", "")
    if not username:
        username = getattr(config, "PEAKENGINE_USERNAME", "")
    password = getattr(config, "NEWPEAK_PASSWORD", "")
    if not password:
        password = getattr(config, "PEAKENGINE_PASSWORD", "")
    headless = getattr(config, "HEADLESS_MODE", False)

    if not username or not password:
        st.warning("⚠️ กรุณากำหนด NEWPEAK_USERNAME / NEWPEAK_PASSWORD (หรือ PEAKENGINE_USERNAME / PEAKENGINE_PASSWORD) ใน config.py")
        return False

    def run_bot():
        bot = None
        try:
            bot = NewPeakBot(use_browser=True, headless=headless)
            _newpeak_bots.append(bot)
            logger.info(f"🆕 เก็บ NewPeakBot instance (ทั้งหมด {len(_newpeak_bots)} ตัว)")

            def log_callback(message: str, status: str = "info"):
                log_func = {
                    "info": logger.info,
                    "success": logger.info,
                    "warning": logger.warning,
                    "error": logger.error,
                }.get(status, logger.info)
                log_func(f"[NewPeakBot] {message}")

            login_success = bot.login(
                username,
                password,
                navigate_after_login=True,
                log_callback=log_callback,
            )

            if login_success:
                logger.info("✅ NewPeakBot Login สำเร็จ สามารถใช้งาน Browser ต่อได้")
            else:
                logger.warning("⚠️ NewPeakBot Login ไม่สำเร็จ กรุณาตรวจสอบ log และข้อมูลใน config.py")
        except Exception as exc:
            logger.error(f"❌ เกิดข้อผิดพลาดใน NewPeakBot: {exc}", exc_info=True)
            if bot and bot._executor:  # type: ignore[attr-defined]
                try:
                    bot.close()
                except Exception:
                    pass

    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(run_bot)
    return True


def wait_for_newpeak_login(bot, timeout: float = 60.0, poll_interval: float = 0.5, log_callback=None) -> bool:
    """รอให้ NewPeakBot login เสร็จก่อนเริ่มประมวลผล"""
    start = time.time()
    while time.time() - start < timeout:
        if getattr(bot, "is_logged_in", False):
            return True
        time.sleep(poll_interval)
    if log_callback:
        try:
            log_callback("⚠️ รอเข้าสู่ระบบ New Peak เกินเวลาที่กำหนด", "warning")
        except Exception:
            pass
    return getattr(bot, "is_logged_in", False)


def wait_for_newpeak_instance(timeout: float = 30.0, poll_interval: float = 0.5):
    """รอให้มีการสร้างอินสแตนซ์ NewPeakBot (จาก thread อื่น)"""
    start = time.time()
    while time.time() - start < timeout:
        if _newpeak_bots:
            bot = _newpeak_bots[-1]
            if bot:
                return bot
        time.sleep(poll_interval)
    return _newpeak_bots[-1] if _newpeak_bots else None

class BankPDFReader:
    """คลาสสำหรับอ่านไฟล์ PDF ของธนาคารต่างๆ"""
    
    def __init__(self):
        self.bank_configs = self.load_bank_configs()
    
    def load_bank_configs(self) -> Dict:
        """โหลดการตั้งค่าสำหรับแต่ละธนาคาร"""
        return {
            "กสิกรไทย": {
                "patterns": {
                    "date": r"(\d{2}/\d{2}/\d{4})",
                    "amount": r"([\d,]+\.\d{2})",
                    "description": r"([A-Za-z0-9\s\-\.]+)",
                    "balance": r"([\d,]+\.\d{2})"
                },
                "columns": ["วันที่", "รายการ", "จำนวนเงิน", "ยอดคงเหลือ"]
            },
            "กรุงเทพ": {
                "patterns": {
                    "date": r"(\d{2}/\d{2}/\d{4})",
                    "amount": r"([\d,]+\.\d{2})",
                    "description": r"([A-Za-z0-9\s\-\.]+)",
                    "balance": r"([\d,]+\.\d{2})"
                },
                "columns": ["วันที่", "รายการ", "จำนวนเงิน", "ยอดคงเหลือ"]
            },
            "กรุงศรี": {
                "patterns": {
                    "date": r"(\d{2}/\d{2}/\d{4})",
                    "amount": r"([\d,]+\.\d{2})",
                    "description": r"([A-Za-z0-9\s\-\.]+)",
                    "balance": r"([\d,]+\.\d{2})"
                },
                "columns": ["วันที่", "รายการ", "จำนวนเงิน", "ยอดคงเหลือ"]
            },
            "กรุงไทย": {
                "patterns": {
                    "date": r"(\d{2}/\d{2}/\d{4})",
                    "amount": r"([\d,]+\.\d{2})",
                    "description": r"([A-Za-z0-9\s\-\.]+)",
                    "balance": r"([\d,]+\.\d{2})"
                },
                "columns": ["วันที่", "รายการ", "จำนวนเงิน", "ยอดคงเหลือ"]
            },
            "TMB": {
                "patterns": {
                    "date": r"(\d{2}/\d{2}/\d{4})",
                    "amount": r"([\d,]+\.\d{2})",
                    "description": r"([A-Za-z0-9\s\-\.]+)",
                    "balance": r"([\d,]+\.\d{2})"
                },
                "columns": ["วันที่", "รายการ", "จำนวนเงิน", "ยอดคงเหลือ"]
            },
            "ธนชาต": {
                "patterns": {
                    "date": r"(\d{2}/\d{2}/\d{4})",
                    "amount": r"([\d,]+\.\d{2})",
                    "description": r"([A-Za-z0-9\s\-\.]+)",
                    "balance": r"([\d,]+\.\d{2})"
                },
                "columns": ["วันที่", "รายการ", "จำนวนเงิน", "ยอดคงเหลือ"]
            }
        }
    
    def extract_text_from_pdf(self, pdf_file) -> str:
        """ดึงข้อความจากไฟล์ PDF"""
        try:
            with pdfplumber.open(pdf_file) as pdf:
                text = ""
                page_texts = []
                for i, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                        page_texts.append({
                            "page_number": i + 1,
                            "text": page_text,
                            "char_count": len(page_text),
                            "line_count": len(page_text.split('\n'))
                        })
                
                # เก็บข้อมูลหน้าไว้สำหรับการวิเคราะห์
                self.pdf_pages_info = page_texts
                return text
        except Exception as e:
            st.error(f"เกิดข้อผิดพลาดในการอ่านไฟล์ PDF: {str(e)}")
            return ""
    
    def extract_tables_from_pdf(self, pdf_file) -> List:
        """ดึงตารางจากไฟล์ PDF"""
        try:
            tables = []
            with pdfplumber.open(pdf_file) as pdf:
                for i, page in enumerate(pdf.pages):
                    page_tables = page.extract_tables()
                    if page_tables:
                        for j, table in enumerate(page_tables):
                            tables.append({
                                "page_number": i + 1,
                                "table_number": j + 1,
                                "table_data": table,
                                "row_count": len(table),
                                "col_count": len(table[0]) if table else 0
                            })
            return tables
        except Exception as e:
            st.error(f"เกิดข้อผิดพลาดในการดึงตาราง: {str(e)}")
            return []
    
    def analyze_kbank_statement(self, text: str) -> Dict:
        """วิเคราะห์ข้อมูลดิบของธนาคารกสิกรไทย"""
        analysis = {
            "account_info": {},
            "transaction_patterns": [],
            "date_ranges": [],
            "amount_patterns": [],
            "keywords": []
        }
        
        # ค้นหาข้อมูลบัญชี
        account_patterns = {
            "account_number": r"เลขที่บัญชี[:\s]*(\d+)",
            "account_name": r"ชื่อบัญชี[:\s]*([^\n]+)",
            "account_type": r"ประเภทบัญชี[:\s]*([^\n]+)",
            "branch": r"สาขา[:\s]*([^\n]+)"
        }
        
        for key, pattern in account_patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                analysis["account_info"][key] = match.group(1).strip()
        
        # ค้นหารูปแบบวันที่
        date_patterns = [
            r"(\d{1,2}/\d{1,2}/\d{4})",
            r"(\d{1,2}-\d{1,2}-\d{4})",
            r"(\d{4}-\d{1,2}-\d{1,2})"
        ]
        
        for pattern in date_patterns:
            dates = re.findall(pattern, text)
            analysis["date_ranges"].extend(dates)
        
        # ค้นหารูปแบบจำนวนเงิน
        amount_patterns = [
            r"([\d,]+\.\d{2})",
            r"([\d,]+\.\d{2})",
            r"([\d,]+)"
        ]
        
        for pattern in amount_patterns:
            amounts = re.findall(pattern, text)
            analysis["amount_patterns"].extend(amounts)
        
        # ค้นหาคำสำคัญ
        keywords = [
            "ถอน", "ฝาก", "โอน", "ชำระ", "รายได้", "รายจ่าย",
            "ยอดคงเหลือ", "ยอดยกมา", "ยอดยกไป", "ดอกเบี้ย",
            "ค่าธรรมเนียม", "ค่าบริการ", "ATM", "POS"
        ]
        
        for keyword in keywords:
            if keyword in text:
                analysis["keywords"].append(keyword)
        
        return analysis
    
    def parse_bank_statement(self, text: str, bank_name: str) -> pd.DataFrame:
        """แปลงข้อความจาก PDF เป็น DataFrame"""
        if bank_name == "กสิกรไทย":
            return self.parse_kbank_statement(text)
        else:
            return self.parse_generic_statement(text, bank_name)
    
    def parse_kbank_statement(self, text: str) -> pd.DataFrame:
        """แปลงข้อความจาก PDF ธนาคารกสิกรไทยเป็น DataFrame"""
        lines = text.split('\n')
        transactions = []
        
        # ค้นหาข้อมูลบัญชี
        account_info = self.extract_account_info(text)
        
        # เก็บยอดคงเหลือก่อนหน้าเพื่อเปรียบเทียบ
        previous_balance = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # รูปแบบ: วันที่ เวลา รายการ จำนวนเงิน ยอดคงเหลือ คำอธิบาย
            # ตัวอย่าง: 01-10-25 11:17 ค่าธรรมเนียม 51.43 22,127,753.64 โอนเข้า/หักบัญชีอัตโนมัติ...
            
            # ค้นหาวันที่ (รูปแบบ DD-MM-YY หรือ DD/MM/YYYY)
            date_match = re.search(r'(\d{2}[-/]\d{2}[-/]\d{2,4})', line)
            if date_match:
                date = date_match.group(1)
                
                # ค้นหาเวลา (รูปแบบ HH:MM)
                time_match = re.search(r'(\d{2}:\d{2})', line)
                time = time_match.group(1) if time_match else ""
                
                # ค้นหาจำนวนเงินทั้งหมด (รูปแบบ 123,456.78)
                amount_matches = re.findall(r'([\d,]+\.\d{2})', line)
                
                if len(amount_matches) >= 2:
                    amount = amount_matches[0]  # จำนวนเงินแรก
                    balance = amount_matches[1]  # ยอดคงเหลือ
                elif len(amount_matches) == 1:
                    amount = amount_matches[0]
                    balance = ""
                else:
                    amount = ""
                    balance = ""
                
                # แยกรายการและคำอธิบาย
                parts = line.split()
                transaction_type = ""
                description = ""
                
                # หาตำแหน่งของจำนวนเงิน
                amount_pos = -1
                for i, part in enumerate(parts):
                    if re.match(r'[\d,]+\.\d{2}', part):
                        amount_pos = i
                        break
                
                # แยกรายการ (ระหว่างวันที่-เวลา กับ จำนวนเงิน) - เอาเวลาออก
                if amount_pos > 2:  # วันที่ เวลา รายการ
                    # ข้ามวันที่และเวลา แล้วเอาเฉพาะรายการ
                    transaction_type = " ".join(parts[2:amount_pos])
                    # เอาเวลาออกจากรายการถ้ามี
                    transaction_type = re.sub(r'\d{2}:\d{2}\s*', '', transaction_type).strip()
                
                # แยกคำอธิบาย (หลังยอดคงเหลือ)
                if len(amount_matches) >= 2:
                    balance_pos = -1
                    for i, part in enumerate(parts):
                        if part == balance:
                            balance_pos = i
                            break
                    
                    if balance_pos > -1 and balance_pos < len(parts) - 1:
                        description = " ".join(parts[balance_pos + 1:])
                
                # กำหนดทิศทางของจำนวนเงิน
                amount_display = amount
                if amount and balance and previous_balance:
                    try:
                        # แปลงจำนวนเงินเป็นตัวเลข
                        current_balance = float(balance.replace(',', ''))
                        prev_balance = float(previous_balance.replace(',', ''))
                        amount_value = float(amount.replace(',', ''))
                        
                        # ถ้ายอดคงเหลือลดลง แสดงจำนวนเงินเป็นติดลบ
                        if current_balance < prev_balance:
                            amount_display = f"({amount})"
                    except:
                        pass
                
                # ตรวจสอบว่าคือค่าธรรมเนียมหรือไม่ และแสดงเป็นยอดลบ
                if transaction_type and any(keyword in transaction_type.lower() for keyword in ['ค่าธรรมเนียม', 'fee', 'charge', 'commission']):
                    if amount and not amount_display.startswith('('):
                        amount_display = f"({amount})"
                
                transactions.append({
                    "วันที่": date,
                    "เวลา": time,
                    "รายการ": transaction_type,
                    "จำนวนเงิน": amount_display,
                    "ยอดคงเหลือ": balance,
                    "คำอธิบาย": description
                })
                
                # อัปเดตยอดคงเหลือก่อนหน้า
                if balance:
                    previous_balance = balance
        
        return pd.DataFrame(transactions)
    
    def extract_account_info(self, text: str) -> Dict:
        """ดึงข้อมูลบัญชีจากข้อความ"""
        account_info = {}
        
        # ค้นหาเลขที่บัญชี
        account_match = re.search(r'เลขที่บัญชีเงินฝาก\s*(\d+-\d+-\d+-\d+)', text)
        if account_match:
            account_info['account_number'] = account_match.group(1)
        
        # ค้นหาชื่อบัญชี
        name_match = re.search(r'ชื่อบัญชี\s*([^\n]+)', text)
        if name_match:
            account_info['account_name'] = name_match.group(1).strip()
        
        # ค้นหาสาขา
        branch_match = re.search(r'สาขาเจ้าของบัญชี\s*([^\n]+)', text)
        if branch_match:
            account_info['branch'] = branch_match.group(1).strip()
        
        # ค้นหาช่วงวันที่
        period_match = re.search(r'รอบระหว่างวันที่\s*(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})', text)
        if period_match:
            account_info['period_start'] = period_match.group(1)
            account_info['period_end'] = period_match.group(2)
        
        # ค้นหายอดยกไป
        balance_match = re.search(r'ยอดยกไป\s*([\d,]+\.\d{2})', text)
        if balance_match:
            account_info['opening_balance'] = balance_match.group(1)
        
        return account_info
    
    def classify_transfer_type(self, description: str) -> str:
        """จำแนกประเภทผู้ส่งโอนเงินจากรายการธุรกรรม"""
        if not description:
            return "อื่นๆ"
        
        description_lower = description.lower()
        
        # บริษัท (บจก.)
        company_keywords = ['บริษัท', 'บจก', 'company', 'co.', 'ltd', 'limited']
        if any(keyword in description_lower for keyword in company_keywords):
            return "บริษัท (บจก.)"
        
        # ห้างหุ้นส่วน (หจก.)
        partnership_keywords = ['ห้างหุ้นส่วน', 'หจก', 'partnership']
        if any(keyword in description_lower for keyword in partnership_keywords):
            return "ห้างหุ้นส่วน (หจก.)"
        
        # บุคคล
        person_keywords = ['นาย', 'นาง', 'น.ส.', 'miss', 'mr', 'mrs', 'ms', 'นส.']
        if any(keyword in description_lower for keyword in person_keywords):
            return "บุคคล"
        
        # ตรวจสอบรูปแบบชื่อบุคคล (ชื่อ-นามสกุล)
        # ถ้ามีรูปแบบที่คล้ายชื่อบุคคล
        if re.search(r'[ก-๙]+\s+[ก-๙]+', description) and len(description.split()) <= 3:
            return "บุคคล"
        
        return "อื่นๆ"
    
    def extract_entity_name(self, description: str) -> str:
        """แยกชื่อบริษัท/บุคคลออกจากคำอธิบาย"""
        if not description:
            return ""
        
        description_lower = description.lower()
        
        # รูปแบบที่พบในธนาคารกสิกรไทย
        patterns = [
            # รูปแบบ: ... บริษัท ชื่อบริษัท ...
            r'บริษัท\s+([ก-๙A-Za-z0-9\s\.\-\+]+?)(?:\s+กร|\s+จำกัด|\s+มหาชน|\s+ฯลฯ|\s+จาก|\s+ถึง|\s+$|$)',
            # รูปแบบ: ... บจก. ชื่อบริษัท ...
            r'บจก\.\s+([ก-๙A-Za-z0-9\s\.\-\+]+?)(?:\s+กร|\s+จำกัด|\s+มหาชน|\s+ฯลฯ|\s+จาก|\s+ถึง|\s+$|$)',
            # รูปแบบ: ... ห้างหุ้นส่วน ชื่อห้าง ...
            r'ห้างหุ้นส่วน\s+([ก-๙A-Za-z0-9\s\.\-\+]+?)(?:\s+กร|\s+จำกัด|\s+มหาชน|\s+ฯลฯ|\s+จาก|\s+ถึง|\s+$|$)',
            # รูปแบบ: ... นาย ชื่อ นามสกุล ...
            r'นาย\s+([ก-๙A-Za-z0-9\s\.\-\+]+?)(?:\s+จาก|\s+ถึง|\s+$|$)',
            # รูปแบบ: ... นาง ชื่อ นามสกุล ...
            r'นาง\s+([ก-๙A-Za-z0-9\s\.\-\+]+?)(?:\s+จาก|\s+ถึง|\s+$|$)',
            # รูปแบบ: ... น.ส. ชื่อ นามสกุล ...
            r'น\.ส\.\s+([ก-๙A-Za-z0-9\s\.\-\+]+?)(?:\s+จาก|\s+ถึง|\s+$|$)',
            # รูปแบบ: ... นส. ชื่อ นามสกุล ...
            r'นส\.\s+([ก-๙A-Za-z0-9\s\.\-\+]+?)(?:\s+จาก|\s+ถึง|\s+$|$)',
            # รูปแบบ: ... Mr. ชื่อ นามสกุล ...
            r'mr\.?\s+([A-Za-z0-9\s\.\-\+]+?)(?:\s+from|\s+to|\s+$|$)',
            # รูปแบบ: ... Miss ชื่อ นามสกุล ...
            r'miss\s+([A-Za-z0-9\s\.\-\+]+?)(?:\s+from|\s+to|\s+$|$)',
            # รูปแบบ: ... Mrs. ชื่อ นามสกุล ...
            r'mrs\.?\s+([A-Za-z0-9\s\.\-\+]+?)(?:\s+from|\s+to|\s+$|$)',
            # รูปแบบ: ... Ms. ชื่อ นามสกุล ...
            r'ms\.?\s+([A-Za-z0-9\s\.\-\+]+?)(?:\s+from|\s+to|\s+$|$)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                entity_name = match.group(1).strip()
                
                # ทำความสะอาดชื่อ
                entity_name = re.sub(r'\s+', ' ', entity_name)  # ลบช่องว่างซ้ำ
                
                # ลบคำที่ไม่ต้องการ
                entity_name = re.sub(r'\bบจก\.?\b', '', entity_name, flags=re.IGNORECASE)  # ลบ "บจก."
                entity_name = re.sub(r'\bบริษัท\b', '', entity_name, flags=re.IGNORECASE)  # ลบ "บริษัท"
                entity_name = re.sub(r'\bห้างหุ้นส่วน\b', '', entity_name, flags=re.IGNORECASE)  # ลบ "ห้างหุ้นส่วน"
                entity_name = re.sub(r'\bจำกัด\b', '', entity_name, flags=re.IGNORECASE)  # ลบ "จำกัด"
                entity_name = re.sub(r'\bมหาชน\b', '', entity_name, flags=re.IGNORECASE)  # ลบ "มหาชน"
                
                # ลบสัญลักษณ์ที่ไม่ต้องการ
                entity_name = re.sub(r'\+\+', '', entity_name)  # ลบ "++"
                entity_name = re.sub(r'\+', '', entity_name)  # ลบ "+"
                entity_name = re.sub(r'ฯลฯ', '', entity_name)  # ลบ "ฯลฯ"
                
                # ลบคำที่ขึ้นต้นด้วยตัวเลขหรือสัญลักษณ์พิเศษ
                entity_name = re.sub(r'^[0-9\-\+\.\s]+', '', entity_name)
                
                # ทำความสะอาดอีกครั้ง
                entity_name = re.sub(r'\s+', ' ', entity_name)  # ลบช่องว่างซ้ำ
                entity_name = entity_name.strip()
                
                # ตรวจสอบว่าไม่ใช่คำที่ยาวเกินไป (อาจเป็นข้อผิดพลาด)
                if len(entity_name) <= 100 and len(entity_name) > 0:
                    return entity_name
        
        return ""
    
    def format_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """แปลงคอลัมน์วันที่เป็นรูปแบบ dd/MM/yyyy"""
        if df.empty or 'วันที่' not in df.columns:
            return df
        
        df_formatted = df.copy()
        
        def convert_date_format(date_str):
            if not date_str or pd.isna(date_str):
                return date_str
            
            try:
                # รูปแบบที่พบในธนาคารกสิกรไทย
                date_patterns = [
                    # DD-MM-YY (เช่น 01-10-25)
                    r'(\d{2})-(\d{2})-(\d{2})',
                    # DD/MM/YYYY (เช่น 01/10/2025)
                    r'(\d{2})/(\d{2})/(\d{4})',
                    # DD-MM-YYYY (เช่น 01-10-2025)
                    r'(\d{2})-(\d{2})-(\d{4})',
                    # YYYY-MM-DD (เช่น 2025-10-01)
                    r'(\d{4})-(\d{2})-(\d{2})',
                ]
                
                for pattern in date_patterns:
                    match = re.match(pattern, str(date_str).strip())
                    if match:
                        if len(match.groups()) == 3:
                            day, month, year = match.groups()
                            
                            # แปลงปี 2 หลักเป็น 4 หลัก
                            if len(year) == 2:
                                year_int = int(year)
                                if year_int >= 0 and year_int <= 30:  # สมมติว่า 00-30 เป็น 2000-2030
                                    year = f"20{year}"
                                else:  # 31-99 เป็น 1931-1999
                                    year = f"19{year}"
                            
                            # สร้างวันที่ในรูปแบบ dd/MM/yyyy
                            return f"{day.zfill(2)}/{month.zfill(2)}/{year}"
                
                # ถ้าไม่ตรงกับรูปแบบใดเลย ให้คืนค่าเดิม
                return date_str
                
            except Exception:
                # ถ้าเกิดข้อผิดพลาด ให้คืนค่าเดิม
                return date_str
        
        # แปลงคอลัมน์วันที่
        df_formatted['วันที่'] = df_formatted['วันที่'].apply(convert_date_format)
        
        return df_formatted
    
    def create_transfer_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """สร้างตารางสรุปข้อมูลแยกตามประเภทผู้ส่งโอน"""
        if df.empty or 'คำอธิบาย' not in df.columns:
            return pd.DataFrame()
        
        # เพิ่มคอลัมน์ประเภทผู้ส่งโอน (ใช้คอลัมน์คำอธิบาย)
        df['ประเภทผู้ส่งโอน'] = df['คำอธิบาย'].apply(self.classify_transfer_type)
        
        # เพิ่มคอลัมน์ชื่อบริษัท/บุคคล
        df['ชื่อบริษัท/บุคคล'] = df['คำอธิบาย'].apply(self.extract_entity_name)
        
        # สร้างตารางสรุป
        summary_data = []
        
        for transfer_type in df['ประเภทผู้ส่งโอน'].unique():
            type_data = df[df['ประเภทผู้ส่งโอน'] == transfer_type]
            
            # คำนวณจำนวนรายการ
            count = len(type_data)
            
            # คำนวณยอดรวม
            total_amount = 0
            positive_amount = 0
            negative_amount = 0
            
            for amount in type_data['จำนวนเงิน']:
                if amount and amount != "":
                    try:
                        # แปลงจำนวนเงินเป็นตัวเลข
                        clean_amount = amount.replace(',', '').replace('(', '').replace(')', '')
                        amount_value = float(clean_amount)
                        
                        if '(' in amount and ')' in amount:
                            # รายการติดลบ
                            total_amount -= amount_value
                            negative_amount += amount_value
                        else:
                            # รายการบวก
                            total_amount += amount_value
                            positive_amount += amount_value
                    except:
                        pass
            
            summary_data.append({
                'ประเภทผู้ส่งโอน': transfer_type,
                'จำนวนรายการ': count,
                'ยอดรวม': f"{total_amount:,.2f}",
                'ยอดเพิ่ม': f"{positive_amount:,.2f}",
                'ยอดลด': f"{negative_amount:,.2f}",
                'ร้อยละ': f"{(count/len(df)*100):.1f}%"
            })
        
        # เรียงลำดับตามจำนวนรายการ (มากไปน้อย)
        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.sort_values('จำนวนรายการ', ascending=False)
        
        return summary_df
    
    def parse_generic_statement(self, text: str, bank_name: str) -> pd.DataFrame:
        """แปลงข้อความจาก PDF ธนาคารอื่นๆ เป็น DataFrame"""
        config = self.bank_configs.get(bank_name, self.bank_configs["กสิกรไทย"])
        
        lines = text.split('\n')
        transactions = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # ค้นหาวันที่
            date_match = re.search(config["patterns"]["date"], line)
            if date_match:
                date = date_match.group(1)
                
                # ค้นหาจำนวนเงิน
                amount_match = re.search(config["patterns"]["amount"], line)
                amount = amount_match.group(1) if amount_match else ""
                
                # ค้นหาคำอธิบาย
                description_match = re.search(config["patterns"]["description"], line)
                description = description_match.group(1) if description_match else ""
                
                # ค้นหายอดคงเหลือ
                balance_match = re.search(config["patterns"]["balance"], line)
                balance = balance_match.group(1) if balance_match else ""
                
                transactions.append({
                    "วันที่": date,
                    "รายการ": description,
                    "จำนวนเงิน": amount,
                    "ยอดคงเหลือ": balance
                })
        
        return pd.DataFrame(transactions)
    
    def detect_bank(self, text: str) -> str:
        """ตรวจสอบว่าเป็นธนาคารไหนจากข้อความ"""
        bank_keywords = {
            "กสิกรไทย": ["กสิกรไทย", "Kasikorn", "KBank"],
            "กรุงเทพ": ["กรุงเทพ", "Bangkok Bank", "BBL"],
            "กรุงศรี": ["กรุงศรี", "Krungsri", "Bank of Ayudhya"],
            "กรุงไทย": ["กรุงไทย", "Krung Thai", "KTB"],
            "TMB": ["TMB", "ธนาคารทหารไทย"],
            "ธนชาต": ["ธนชาต", "Thanachart", "TBank"]
        }
        
        text_lower = text.lower()
        for bank, keywords in bank_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    return bank
        
        return "กสิกรไทย"  # default

def process_pdf_file(uploaded_file, reader, selected_bank):
    """ประมวลผลไฟล์ PDF และแสดงผลลัพธ์"""
    with st.spinner("กำลังประมวลผลไฟล์ PDF..."):
        # ดึงข้อความจาก PDF
        text = reader.extract_text_from_pdf(uploaded_file)
        
        if text:
            # ตรวจสอบธนาคารอัตโนมัติ
            detected_bank = reader.detect_bank(text)
            st.info(f"🔍 ตรวจพบธนาคาร: {detected_bank}")
            
            # แสดงข้อมูลดิบและวิเคราะห์
            st.header("🔍 ข้อมูลดิบจาก PDF")
            
            # แสดงข้อมูลหน้าแต่ละหน้า
            if hasattr(reader, 'pdf_pages_info'):
                st.subheader("📄 ข้อมูลแต่ละหน้า")
                for page_info in reader.pdf_pages_info:
                    with st.expander(f"หน้า {page_info['page_number']} ({page_info['char_count']} ตัวอักษร, {page_info['line_count']} บรรทัด)"):
                        # แสดงข้อมูลดิบในรูปแบบตาราง
                        lines = page_info['text'].split('\n')
                        if lines:
                            # สร้าง DataFrame สำหรับแสดงข้อมูลดิบ
                            raw_data = []
                            for i, line in enumerate(lines):
                                if line.strip():
                                    raw_data.append({
                                        "บรรทัด": i + 1,
                                        "เนื้อหา": line.strip()
                                    })
                            
                            if raw_data:
                                df_raw = pd.DataFrame(raw_data)
                                st.dataframe(df_raw, use_container_width=True, height=300)
                            else:
                                st.text(page_info['text'])
            
            # ดึงตารางจาก PDF
            tables = reader.extract_tables_from_pdf(uploaded_file)
            if tables:
                st.subheader("📊 ตารางที่พบใน PDF")
                for table_info in tables:
                    with st.expander(f"ตารางที่ {table_info['table_number']} ในหน้า {table_info['page_number']} ({table_info['row_count']} แถว, {table_info['col_count']} คอลัมน์)"):
                        if table_info['table_data']:
                            df_table = pd.DataFrame(table_info['table_data'])
                            st.dataframe(df_table, use_container_width=True)
            
            # วิเคราะห์ข้อมูลดิบสำหรับธนาคารกสิกรไทย
            if detected_bank == "กสิกรไทย":
                st.subheader("🏦 ข้อมูลบัญชีธนาคารกสิกรไทย")
                
                # แสดงข้อมูลบัญชี
                account_info = reader.extract_account_info(text)
                if account_info:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**ข้อมูลบัญชี:**")
                        for key, value in account_info.items():
                            if key == 'account_number':
                                st.write(f"- เลขที่บัญชี: {value}")
                            elif key == 'account_name':
                                st.write(f"- ชื่อบัญชี: {value}")
                            elif key == 'branch':
                                st.write(f"- สาขา: {value}")
                            elif key == 'period_start':
                                st.write(f"- วันที่เริ่มต้น: {value}")
                            elif key == 'period_end':
                                st.write(f"- วันที่สิ้นสุด: {value}")
                            elif key == 'opening_balance':
                                st.write(f"- ยอดยกไป: {value}")
                    
                    with col2:
                        st.write("**สรุปข้อมูล:**")
                        st.write(f"- จำนวนบรรทัดทั้งหมด: {len(text.split('\n'))}")
                        st.write(f"- จำนวนตัวอักษร: {len(text)}")
                        
                        # ค้นหาธุรกรรม
                        transaction_lines = []
                        for line in text.split('\n'):
                            if re.search(r'\d{2}[-/]\d{2}[-/]\d{2,4}', line):
                                transaction_lines.append(line)
                        
                        st.write(f"- จำนวนธุรกรรมที่พบ: {len(transaction_lines)}")
                
                # แสดงการวิเคราะห์เพิ่มเติม
                analysis = reader.analyze_kbank_statement(text)
                
                if analysis["date_ranges"]:
                    st.write(f"**วันที่ที่พบ:** {len(analysis['date_ranges'])} รายการ")
                    unique_dates = list(set(analysis["date_ranges"]))[:10]
                    st.write(f"ตัวอย่าง: {', '.join(unique_dates)}")
                
                if analysis["amount_patterns"]:
                    st.write(f"**จำนวนเงินที่พบ:** {len(analysis['amount_patterns'])} รายการ")
                    unique_amounts = list(set(analysis["amount_patterns"]))[:10]
                    st.write(f"ตัวอย่าง: {', '.join(unique_amounts)}")
                
                if analysis["keywords"]:
                    st.write(f"**คำสำคัญที่พบ:** {', '.join(analysis['keywords'])}")
            
            # แปลงข้อมูล
            df = reader.parse_bank_statement(text, detected_bank)
            
            # แปลงรูปแบบวันที่เป็น dd/MM/yyyy
            df = reader.format_date_column(df)

            def parse_amount_value(amount_str):
                if amount_str is None or (isinstance(amount_str, float) and pd.isna(amount_str)):
                    return None
                text_amount = str(amount_str).strip()
                if not text_amount:
                    return None
                negative = False
                if text_amount.startswith('(') and text_amount.endswith(')'):
                    negative = True
                    text_amount = text_amount[1:-1]
                text_amount = text_amount.replace(',', '').replace('+', '').strip()
                try:
                    value = float(text_amount)
                    return -value if negative else value
                except ValueError:
                    return None

            if not df.empty and 'จำนวนเงิน' in df.columns:
                df['ยอดเงิน_numeric'] = df['จำนวนเงิน'].apply(parse_amount_value)
            else:
                df['ยอดเงิน_numeric'] = pd.Series(dtype=float)

            if not df.empty and 'คำอธิบาย' in df.columns:
                df['ประเภทผู้ส่งโอน'] = df['คำอธิบาย'].apply(reader.classify_transfer_type)
                st.subheader("🏷️ ประเภทผู้ส่งโอนที่พบ")
                category_counts = df['ประเภทผู้ส่งโอน'].value_counts()
                category_summary = pd.DataFrame({
                    'ประเภทผู้ส่งโอน': category_counts.index,
                    'จำนวนรายการ': category_counts.values
                })
                st.dataframe(category_summary, use_container_width=True, hide_index=True)

            if not df.empty and 'ยอดเงิน_numeric' in df.columns:
                income_df = df[df['ยอดเงิน_numeric'] > 0]
                expense_df = df[df['ยอดเงิน_numeric'] < 0]
                st.subheader("💰 สรุปเงินเข้า/เงินออก")
                col_in, col_out, col_net = st.columns(3)
                with col_in:
                    st.metric("เงินเข้า (Income)", f"{income_df['ยอดเงิน_numeric'].sum():,.2f}", help="ผลรวมยอดเงินที่เป็นบวก")
                    st.caption(f"รายการเงินเข้า: {len(income_df)}")
                with col_out:
                    st.metric("เงินออก (Expense)", f"{expense_df['ยอดเงิน_numeric'].sum():,.2f}", help="ผลรวมยอดเงินที่เป็นลบ")
                    st.caption(f"รายการเงินออก: {len(expense_df)}")
                with col_net:
                    net_amount = df['ยอดเงิน_numeric'].sum()
                    st.metric("ยอดสุทธิ", f"{net_amount:,.2f}", help="ยอดเงินเข้า - เงินออก")
            
            if not df.empty:
                st.success("✅ ประมวลผลสำเร็จ!")
                
                # แสดงข้อมูลเกี่ยวกับการแปลงวันที่
                if 'วันที่' in df.columns:
                    st.info("📅 วันที่ได้ถูกแปลงเป็นรูปแบบ dd/MM/yyyy แล้ว")
                
                # แสดงข้อมูลตัวอย่าง
                st.header("📊 ข้อมูลที่ประมวลผลได้")
                
                # แสดงข้อมูลทั้งหมดในรูปแบบตารางพร้อมการจัดรูปแบบสี (ไม่แสดงคอลัมน์เวลา)
                display_columns = ['วันที่', 'รายการ', 'จำนวนเงิน', 'ยอดคงเหลือ']
                if 'คำอธิบาย' in df.columns:
                    display_columns.append('คำอธิบาย')
                if 'ประเภทผู้ส่งโอน' in df.columns:
                    display_columns.append('ประเภทผู้ส่งโอน')
                
                # เพิ่มคอลัมน์ชื่อบริษัท/บุคคลถ้ามีคำอธิบาย
                if 'คำอธิบาย' in df.columns:
                    df_display = df.copy()
                    df_display['ชื่อบริษัท/บุคคล'] = df_display['คำอธิบาย'].apply(reader.extract_entity_name)
                    if 'ชื่อบริษัท/บุคคล' in df_display.columns:
                        display_columns.append('ชื่อบริษัท/บุคคล')
                else:
                    df_display = df
                
                available_columns = [col for col in display_columns if col in df_display.columns]
                
                st.dataframe(
                    df_display[available_columns], 
                    use_container_width=True, 
                    height=400,
                    column_config={
                        "วันที่": st.column_config.TextColumn(
                            "วันที่",
                            help="วันที่ทำธุรกรรม (รูปแบบ dd/MM/yyyy)"
                        ),
                        "จำนวนเงิน": st.column_config.TextColumn(
                            "จำนวนเงิน",
                            help="จำนวนเงินที่ทำธุรกรรม (วงเล็บหมายถึงรายการที่ยอดลดลง)"
                        ),
                        "ชื่อบริษัท/บุคคล": st.column_config.TextColumn(
                            "ชื่อบริษัท/บุคคล",
                            help="ชื่อบริษัทหรือบุคคลที่ทำธุรกรรม (แยกจากคำอธิบาย)"
                        )
                    }
                )
                
                # สถิติข้อมูล
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("จำนวนรายการ", len(df))
                with col2:
                    st.metric("วันที่เริ่มต้น", df['วันที่'].min() if 'วันที่' in df.columns else "N/A")
                with col3:
                    st.metric("วันที่สิ้นสุด", df['วันที่'].max() if 'วันที่' in df.columns else "N/A")
                with col4:
                    # คำนวณยอดรวมและแยกรายการเพิ่ม/ลด
                    if 'ยอดเงิน_numeric' in df.columns and not df['ยอดเงิน_numeric'].isna().all():
                        total_amount = df['ยอดเงิน_numeric'].sum()
                        positive_count = int((df['ยอดเงิน_numeric'] > 0).sum())
                        negative_count = int((df['ยอดเงิน_numeric'] < 0).sum())
                        
                        st.metric("ยอดรวม", f"{total_amount:,.2f}")
                        
                        # แสดงสถิติเพิ่มเติม
                        st.write(f"📈 รายการเพิ่ม: {positive_count}")
                        st.write(f"📉 รายการลด: {negative_count}")
                    else:
                        st.metric("ยอดรวม", "N/A")
                
                # แสดงข้อมูลแยกตามประเภท
                if 'รายการ' in df.columns:
                    st.subheader("📈 สรุปข้อมูลตามประเภท")
                    transaction_summary = df['รายการ'].value_counts()
                    st.bar_chart(transaction_summary)
                
                # แสดงสรุปข้อมูลแยกตามประเภทผู้ส่งโอน
                if 'คำอธิบาย' in df.columns:
                    st.subheader("🏢 สรุปข้อมูลแยกตามประเภทผู้ส่งโอน")
                    
                    # สร้างตารางสรุป
                    transfer_summary = reader.create_transfer_summary(df.copy())
                    
                    if not transfer_summary.empty:
                        # แสดงตารางสรุป
                        st.dataframe(
                            transfer_summary,
                            use_container_width=True,
                            column_config={
                                "ประเภทผู้ส่งโอน": st.column_config.TextColumn(
                                    "ประเภทผู้ส่งโอน",
                                    help="ประเภทของผู้ส่งโอนเงิน"
                                ),
                                "จำนวนรายการ": st.column_config.NumberColumn(
                                    "จำนวนรายการ",
                                    help="จำนวนรายการธุรกรรม"
                                ),
                                "ยอดรวม": st.column_config.TextColumn(
                                    "ยอดรวม",
                                    help="ยอดเงินรวมทั้งหมด"
                                ),
                                "ยอดเพิ่ม": st.column_config.TextColumn(
                                    "ยอดเพิ่ม",
                                    help="ยอดเงินที่เพิ่มเข้ามา"
                                ),
                                "ยอดลด": st.column_config.TextColumn(
                                    "ยอดลด",
                                    help="ยอดเงินที่ลดลง"
                                ),
                                "ร้อยละ": st.column_config.TextColumn(
                                    "ร้อยละ",
                                    help="เปอร์เซ็นต์ของจำนวนรายการ"
                                )
                            }
                        )
                        
                        # แสดงกราฟแท่ง
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("**📊 กราฟจำนวนรายการ:**")
                            chart_data = transfer_summary.set_index('ประเภทผู้ส่งโอน')['จำนวนรายการ']
                            st.bar_chart(chart_data)
                        
                        with col2:
                            st.write("**💰 กราฟยอดรวม:**")
                            # แปลงยอดรวมเป็นตัวเลขสำหรับกราฟ
                            chart_amounts = []
                            for amount_str in transfer_summary['ยอดรวม']:
                                try:
                                    amount_value = float(amount_str.replace(',', ''))
                                    chart_amounts.append(amount_value)
                                except:
                                    chart_amounts.append(0)
                            
                            chart_df = pd.DataFrame({
                                'ประเภทผู้ส่งโอน': transfer_summary['ประเภทผู้ส่งโอน'],
                                'ยอดรวม': chart_amounts
                            }).set_index('ประเภทผู้ส่งโอน')
                            
                            st.bar_chart(chart_df['ยอดรวม'])
                        
                        # แสดงรายละเอียดแต่ละประเภท
                        st.subheader("📋 รายละเอียดแต่ละประเภท")
                        
                        for transfer_type in transfer_summary['ประเภทผู้ส่งโอน']:
                            type_count = transfer_summary[transfer_summary['ประเภทผู้ส่งโอน'] == transfer_type]['จำนวนรายการ'].iloc[0]
                            type_total = transfer_summary[transfer_summary['ประเภทผู้ส่งโอน'] == transfer_type]['ยอดรวม'].iloc[0]
                            
                            with st.expander(f"🔍 {transfer_type} ({type_count} รายการ, ยอดรวม: {type_total})"):
                                type_data = df[df['คำอธิบาย'].apply(reader.classify_transfer_type) == transfer_type]
                                
                                if not type_data.empty:
                                    # แสดงสถิติย่อย
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.metric("จำนวนรายการ", type_count)
                                    with col2:
                                        st.metric("ยอดรวม", type_total)
                                    with col3:
                                        st.metric("ร้อยละ", transfer_summary[transfer_summary['ประเภทผู้ส่งโอน'] == transfer_type]['ร้อยละ'].iloc[0])
                                    
                                    st.markdown("---")
                                    
                                    # แสดงรายการธุรกรรม
                                    st.write(f"**รายการธุรกรรมทั้งหมด ({type_count} รายการ):**")
                                    
                                    # เพิ่มตัวกรองข้อมูล
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        # กรองตามวันที่
                                        if 'วันที่' in type_data.columns:
                                            unique_dates = sorted(type_data['วันที่'].unique())
                                            selected_dates = st.multiselect(
                                                "เลือกวันที่:",
                                                unique_dates,
                                                default=unique_dates,
                                                key=f"date_filter_{transfer_type}"
                                            )
                                            if selected_dates:
                                                type_data = type_data[type_data['วันที่'].isin(selected_dates)]
                                    
                                    with col2:
                                        # กรองตามจำนวนเงิน
                                        amount_filter = st.selectbox(
                                            "กรองตามจำนวนเงิน:",
                                            ["ทั้งหมด", "รายการเพิ่ม", "รายการลด"],
                                            key=f"amount_filter_{transfer_type}"
                                        )
                                        if 'ยอดเงิน_numeric' in type_data.columns:
                                            if amount_filter == "รายการเพิ่ม":
                                                type_data = type_data[type_data['ยอดเงิน_numeric'] > 0]
                                            elif amount_filter == "รายการลด":
                                                type_data = type_data[type_data['ยอดเงิน_numeric'] < 0]
                                        else:
                                            if amount_filter == "รายการเพิ่ม":
                                                type_data = type_data[type_data['จำนวนเงิน'].str.contains(r'^\d', na=False)]
                                            elif amount_filter == "รายการลด":
                                                type_data = type_data[type_data['จำนวนเงิน'].str.contains(r'^\(', na=False)]
                                    
                                    # เลือกคอลัมน์ที่ต้องการแสดง
                                    display_columns = ['วันที่', 'รายการ', 'จำนวนเงิน', 'ยอดคงเหลือ']
                                    if 'คำอธิบาย' in type_data.columns:
                                        display_columns.append('คำอธิบาย')
                                    if 'ชื่อบริษัท/บุคคล' in type_data.columns:
                                        display_columns.append('ชื่อบริษัท/บุคคล')
                                    
                                    available_columns = [col for col in display_columns if col in type_data.columns]
                                    
                                    # แสดงจำนวนรายการที่กรองแล้ว
                                    filtered_count = len(type_data)
                                    if filtered_count != type_count:
                                        st.info(f"📊 แสดง {filtered_count} รายการจากทั้งหมด {type_count} รายการ")
                                    
                                    # แสดงตารางพร้อมการจัดรูปแบบ
                                    st.dataframe(
                                        type_data[available_columns], 
                                        use_container_width=True,
                                        height=400,
                                        column_config={
                                            "วันที่": st.column_config.TextColumn(
                                                "วันที่",
                                                help="วันที่ทำธุรกรรม (รูปแบบ dd/MM/yyyy)"
                                            ),
                                            "รายการ": st.column_config.TextColumn(
                                                "รายการ",
                                                help="ประเภทธุรกรรม"
                                            ),
                                            "จำนวนเงิน": st.column_config.TextColumn(
                                                "จำนวนเงิน",
                                                help="จำนวนเงิน (วงเล็บหมายถึงรายการที่ยอดลดลง)"
                                            ),
                                            "ยอดคงเหลือ": st.column_config.TextColumn(
                                                "ยอดคงเหลือ",
                                                help="ยอดเงินคงเหลือหลังทำธุรกรรม"
                                            ),
                                            "คำอธิบาย": st.column_config.TextColumn(
                                                "คำอธิบาย",
                                                help="รายละเอียดเพิ่มเติมของธุรกรรม"
                                            ),
                                            "ชื่อบริษัท/บุคคล": st.column_config.TextColumn(
                                                "ชื่อบริษัท/บุคคล",
                                                help="ชื่อบริษัทหรือบุคคลที่ทำธุรกรรม (แยกจากคำอธิบาย)"
                                            )
                                        }
                                    )
                                    
                                    # แสดงสรุปย่อย
                                    st.markdown("---")
                                    st.write("**สรุปย่อย:**")
                                    
                                    # คำนวณสถิติเพิ่มเติม
                                    if 'ยอดเงิน_numeric' in type_data.columns:
                                        positive_count = int((type_data['ยอดเงิน_numeric'] > 0).sum())
                                        negative_count = int((type_data['ยอดเงิน_numeric'] < 0).sum())
                                    else:
                                        positive_count = len(type_data[type_data['จำนวนเงิน'].str.contains(r'^\d', na=False)])
                                        negative_count = len(type_data[type_data['จำนวนเงิน'].str.contains(r'^\(', na=False)])
                                    
                                    col1, col2, col3, col4 = st.columns(4)
                                    with col1:
                                        st.write(f"📈 รายการเพิ่ม: {positive_count}")
                                    with col2:
                                        st.write(f"📉 รายการลด: {negative_count}")
                                    with col3:
                                        st.write(f"📅 วันที่เริ่ม: {type_data['วันที่'].min()}")
                                    with col4:
                                        st.write(f"📅 วันที่สิ้นสุด: {type_data['วันที่'].max()}")
                                        
                                else:
                                    st.write("ไม่มีข้อมูล")
                    else:
                        st.write("ไม่พบข้อมูลการจำแนกประเภทผู้ส่งโอน")
                
                # แสดงข้อมูลแยกตามทิศทาง (เพิ่ม/ลด)
                if 'จำนวนเงิน' in df.columns:
                    st.subheader("📊 สรุปข้อมูลตามทิศทาง")
                    
                    # แยกข้อมูลตามทิศทาง
                    positive_transactions = df[df['ยอดเงิน_numeric'] > 0] if 'ยอดเงิน_numeric' in df.columns else pd.DataFrame()
                    negative_transactions = df[df['ยอดเงิน_numeric'] < 0] if 'ยอดเงิน_numeric' in df.columns else pd.DataFrame()
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**📈 รายการที่ยอดเพิ่ม:**")
                        if not positive_transactions.empty:
                            # เลือกคอลัมน์ที่ต้องการแสดง (ไม่รวมเวลา)
                            display_columns = ['วันที่', 'รายการ', 'จำนวนเงิน', 'ยอดคงเหลือ']
                            available_columns = [col for col in display_columns if col in positive_transactions.columns]
                            st.dataframe(positive_transactions[available_columns], use_container_width=True)
                        else:
                            st.write("ไม่มีรายการที่ยอดเพิ่ม")
                    
                    with col2:
                        st.write("**📉 รายการที่ยอดลด:**")
                        if not negative_transactions.empty:
                            # เลือกคอลัมน์ที่ต้องการแสดง (ไม่รวมเวลา)
                            display_columns = ['วันที่', 'รายการ', 'จำนวนเงิน', 'ยอดคงเหลือ']
                            available_columns = [col for col in display_columns if col in negative_transactions.columns]
                            st.dataframe(negative_transactions[available_columns], use_container_width=True)
                        else:
                            st.write("ไม่มีรายการที่ยอดลด")
                
                # แสดงข้อมูลแยกตามวันที่
                if 'วันที่' in df.columns:
                    st.subheader("📅 สรุปข้อมูลตามวันที่")
                    date_summary = df['วันที่'].value_counts().sort_index()
                    st.line_chart(date_summary)
                
                # ดาวน์โหลดไฟล์ Excel
                st.header("💾 ดาวน์โหลดไฟล์ Excel")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # สร้างไฟล์ Excel ข้อมูลดิบ
                    output_raw = io.BytesIO()
                    with pd.ExcelWriter(output_raw, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name='Bank_Statement', index=False)
                    
                    output_raw.seek(0)
                    
                    st.download_button(
                        label="📥 ดาวน์โหลดข้อมูลดิบ",
                        data=output_raw.getvalue(),
                        file_name=f"bank_statement_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        help="ข้อมูลธุรกรรมทั้งหมดโดยไม่มีการจำแนกประเภท",
                        key="download_raw_pdf"
                    )
                
                with col2:
                    # สร้างไฟล์ Excel ข้อมูลที่จำแนกแล้ว
                    if 'คำอธิบาย' in df.columns:
                        # เพิ่มคอลัมน์ประเภทผู้ส่งโอนและชื่อบริษัท/บุคคล
                        df_classified = df.copy()
                        df_classified['ประเภทผู้ส่งโอน'] = df_classified['คำอธิบาย'].apply(reader.classify_transfer_type)
                        df_classified['ชื่อบริษัท/บุคคล'] = df_classified['คำอธิบาย'].apply(reader.extract_entity_name)
                        
                        # สร้างตารางสรุป
                        transfer_summary = reader.create_transfer_summary(df.copy())
                        
                        output_classified = io.BytesIO()
                        with pd.ExcelWriter(output_classified, engine='openpyxl') as writer:
                            # Sheet 1: ข้อมูลที่จำแนกแล้ว
                            df_classified.to_excel(writer, sheet_name='ข้อมูลจำแนกแล้ว', index=False)
                            
                            # Sheet 2: สรุปตามประเภท
                            if not transfer_summary.empty:
                                transfer_summary.to_excel(writer, sheet_name='สรุปตามประเภท', index=False)
                            
                            # Sheet 3: แยกตามประเภทผู้ส่งโอน
                            for transfer_type in df_classified['ประเภทผู้ส่งโอน'].unique():
                                type_data = df_classified[df_classified['ประเภทผู้ส่งโอน'] == transfer_type]
                                sheet_name = f"ประเภท_{transfer_type}"[:31]  # Excel sheet name limit
                                type_data.to_excel(writer, sheet_name=sheet_name, index=False)
                        
                        output_classified.seek(0)
                        
                        st.download_button(
                            label="📊 ดาวน์โหลดข้อมูลจำแนกแล้ว",
                            data=output_classified.getvalue(),
                            file_name=f"bank_statement_classified_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            help="ข้อมูลธุรกรรมที่จำแนกตามประเภทผู้ส่งโอน พร้อมสรุปและแยกตามประเภท",
                            key="download_classified_pdf"
                        )
                    else:
                        st.info("ไม่สามารถจำแนกประเภทได้เนื่องจากไม่มีคอลัมน์คำอธิบาย")
            else:
                st.warning("⚠️ ไม่พบข้อมูลธุรกรรมในไฟล์ PDF")
                
                # แสดงข้อมูลดิบทั้งหมดในรูปแบบตาราง
                st.subheader("📝 ข้อมูลดิบทั้งหมด")
                
                # แปลงข้อมูลดิบเป็นตาราง
                lines = text.split('\n')
                raw_data = []
                for i, line in enumerate(lines):
                    if line.strip():
                        raw_data.append({
                            "บรรทัด": i + 1,
                            "เนื้อหา": line.strip()
                        })
                
                if raw_data:
                    df_raw = pd.DataFrame(raw_data)
                    st.dataframe(df_raw, use_container_width=True, height=400)
                else:
                    st.text_area("เนื้อหาจาก PDF:", text, height=400)
        else:
            st.error("❌ ไม่สามารถอ่านไฟล์ PDF ได้")

def render_statement_page(reader, selected_bank):
    """หน้า Statement - ประมวลผล PDF และ Excel ของธนาคาร"""
    st.header("📄 Statement - ประมวลผล Statement ธนาคาร")
    st.markdown("---")
    
    # แท็บสำหรับอัปโหลด PDF และ Excel
    tab1, tab2 = st.tabs(["📄 อัปโหลดไฟล์ PDF", "📊 อัปโหลดไฟล์ Excel"])
    
    uploaded_file = None
    
    with tab1:
        st.write("**อัปโหลดไฟล์ PDF ของธนาคาร**")
        
        # อัปโหลดไฟล์ PDF
        st.subheader("📁 อัปโหลดไฟล์ PDF")
        uploaded_file = st.file_uploader(
            "เลือกไฟล์ PDF ของธนาคาร",
            type=['pdf'],
            help="อัปโหลดไฟล์ PDF ของธนาคารเพื่อประมวลผลข้อมูล",
            key="pdf_upload_statement"
        )
        
        # ประมวลผลไฟล์ PDF ถ้ามีการอัปโหลด
        if uploaded_file is not None:
            st.success(f"✅ อัปโหลดไฟล์สำเร็จ: {uploaded_file.name}")
            
            # แสดงข้อมูลไฟล์
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("ขนาดไฟล์", f"{uploaded_file.size / 1024:.1f} KB")
            with col2:
                st.metric("ประเภทไฟล์", uploaded_file.type)
            with col3:
                st.metric("ธนาคารที่เลือก", selected_bank)
            
            # ปุ่มประมวลผล
            if st.button("🔄 ประมวลผลไฟล์ PDF", type="primary", key="process_pdf_btn"):
                process_pdf_file(uploaded_file, reader, selected_bank)
    
    with tab2:
        st.write("**อัปโหลดไฟล์ Excel ที่มีข้อมูลบริษัท**")
        
        # อัปโหลดไฟล์ Excel
        st.subheader("📊 อัปโหลดไฟล์ Excel")
        uploaded_excel = st.file_uploader(
            "เลือกไฟล์ Excel",
            type=['xlsx', 'xls'],
            help="ไฟล์ Excel ที่มีคอลัมน์ชื่อบริษัท/บุคคล",
            key="excel_upload_statement"
        )
        
        if uploaded_excel is not None:
            try:
                # อ่านไฟล์ Excel
                df_excel = pd.read_excel(uploaded_excel)
                
                st.success("✅ อัปโหลดไฟล์ Excel สำเร็จ!")
                
                # แสดงข้อมูลตัวอย่าง
                st.subheader("📊 ข้อมูลตัวอย่าง")
                st.write(f"จำนวนแถว: {len(df_excel)}")
                st.write(f"จำนวนคอลัมน์: {len(df_excel.columns)}")
                
                # แสดงคอลัมน์ที่มีอยู่
                st.write("**คอลัมน์ที่มีอยู่:**")
                st.write(", ".join(df_excel.columns.tolist()))
                
                # เลือกคอลัมน์ที่มีชื่อบริษัท
                company_columns = [col for col in df_excel.columns if any(keyword in col.lower() for keyword in ['บริษัท', 'ชื่อ', 'company', 'name'])]
                
                if company_columns:
                    selected_column = st.selectbox(
                        "เลือกคอลัมน์ที่มีชื่อบริษัท/บุคคล:",
                        company_columns,
                        help="เลือกคอลัมน์ที่มีชื่อบริษัทหรือบุคคล",
                        key="company_column_statement"
                    )
                    
                    if selected_column:
                        # แสดงข้อมูลตัวอย่าง
                        st.subheader("📋 ข้อมูลตัวอย่าง")
                        st.dataframe(df_excel.head(10), use_container_width=True)
                        
                        # ปุ่มดึงข้อมูล DBD
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if st.button("🔍 ดึงข้อมูล DBD จาก Excel", type="primary", key="fetch_dbd_excel_statement"):
                                # แสดงข้อมูลก่อนเริ่มการทำงาน
                                st.subheader("🚀 เริ่มการดึงข้อมูลจาก DBD DataWarehouse")
                                
                                # แสดงข้อมูลที่จะประมวลผล
                                st.write(f"**จำนวนบริษัทที่จะประมวลผล:** {len(df_excel[df_excel[selected_column].notna() & (df_excel[selected_column] != '')])}")
                                st.write(f"**คอลัมน์ที่ใช้:** {selected_column}")
                                
                                # แสดงตัวอย่างข้อมูล
                                st.write("**ตัวอย่างข้อมูลที่จะประมวลผล:**")
                                sample_data = df_excel[df_excel[selected_column].notna() & (df_excel[selected_column] != '')][selected_column].head(5)
                                for i, company in enumerate(sample_data, 1):
                                    st.write(f"{i}. {company}")
                                
                                st.markdown("---")
                                
                                with st.spinner("กำลังดึงข้อมูลจาก DBD DataWarehouse..."):
                                    try:
                                        # ดึงข้อมูลจาก DBD
                                        use_browser_mode = st.session_state.get('use_browser_mode', True)
                                        headless_mode = st.session_state.get('headless_mode', False)
                                        
                                        df_excel_with_dbd = integrate_with_streamlit(
                                            df_excel.copy(), 
                                            selected_column,
                                            use_browser=use_browser_mode,
                                            headless=headless_mode
                                        )
                                        
                                        # แสดงผลลัพธ์
                                        st.success("✅ ดึงข้อมูลจาก DBD DataWarehouse สำเร็จ!")
                                        
                                        # แสดงตารางสรุปข้อมูล DBD
                                        dbd_summary = create_dbd_summary_table(df_excel_with_dbd)
                                        
                                        if not dbd_summary.empty:
                                            st.subheader("📋 สรุปข้อมูล DBD")
                                            st.dataframe(dbd_summary, use_container_width=True)
                                        
                                        # สร้างไฟล์ Excel พร้อมข้อมูล DBD
                                        output_dbd = io.BytesIO()
                                        with pd.ExcelWriter(output_dbd, engine='openpyxl') as writer:
                                            df_excel_with_dbd.to_excel(writer, sheet_name='ข้อมูลพร้อม DBD', index=False)
                                            
                                            if not dbd_summary.empty:
                                                dbd_summary.to_excel(writer, sheet_name='สรุปข้อมูล DBD', index=False)
                                        
                                        output_dbd.seek(0)
                                        
                                        st.download_button(
                                            label="📥 ดาวน์โหลดข้อมูลพร้อม DBD",
                                            data=output_dbd.getvalue(),
                                            file_name=f"excel_with_dbd_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            help="ข้อมูล Excel พร้อมข้อมูลจาก DBD DataWarehouse",
                                            key="download_dbd_excel_statement"
                                        )
                                        
                                    except Exception as e:
                                        st.error(f"❌ เกิดข้อผิดพลาดในการดึงข้อมูล: {str(e)}")
                        
                        with col2:
                            st.info("""
                            **หมายเหตุ:**
                            • การดึงข้อมูลอาจใช้เวลานานขึ้นอยู่กับจำนวนบริษัท
                            • ระบบจะหน่วงเวลา 0.5 วินาทีระหว่างการค้นหาแต่ละบริษัท
                            • ข้อมูลจะถูกดึงจาก [DBD DataWarehouse](https://datawarehouse.dbd.go.th/index)
                            """)
                else:
                    st.warning("⚠️ ไม่พบคอลัมน์ที่มีชื่อบริษัท/บุคคล")
                    st.write("**คอลัมน์ที่มีอยู่:**")
                    for col in df_excel.columns:
                        st.write(f"• {col}")
            
            except Exception as e:
                st.error(f"❌ เกิดข้อผิดพลาดในการอ่านไฟล์ Excel: {str(e)}")
    
    return uploaded_file, selected_bank

def render_dbd_bot_page(reader):
    """หน้า Bot ดึงข้อมูลกรมพัฒน์ - ดึงข้อมูลจาก DBD"""
    st.header("🤖 Bot ดึงข้อมูลกรมพัฒน์")
    st.markdown("---")
    st.write("**ค้นหาข้อมูลบริษัทจาก DBD DataWarehouse**")
    
    # แสดงสถานะการตั้งค่า
    use_browser_mode = st.session_state.get('use_browser_mode', True)
    headless_mode = st.session_state.get('headless_mode', False)
    
    if use_browser_mode:
        if headless_mode:
            st.info("🌐 โหมด: Chromium Browser (Headless) - Browser จะทำงานแบบซ่อนหน้าจอ")
        else:
            st.success("🌐 โหมด: Chromium Browser (แสดงหน้าจอ) - 👀 ตรวจสอบ Chromium window ที่เปิดอยู่")
    else:
        st.info("📡 โหมด: Requests Mode - ใช้ requests library ธรรมดา")
    
    st.markdown("---")
    
    # เลือกโหมดการใช้งาน
    mode = st.radio(
        "เลือกโหมด:",
        ["🔍 ค้นหาบริษัทเดี่ยว", "📊 อัปโหลดไฟล์ Excel"],
        help="เลือกวิธีการใช้งานบอท DBD",
        key="dbd_bot_mode"
    )
    
    if mode == "🔍 ค้นหาบริษัทเดี่ยว":
        st.subheader("🔍 ค้นหาข้อมูลบริษัท")
        
        # ช่องค้นหา
        company_name = st.text_input(
            "ชื่อบริษัท/บุคคล:",
            placeholder="ตัวอย่าง: ทรอเวลล์ กร",
            help="กรอกชื่อบริษัทหรือบุคคลที่ต้องการค้นหา",
            key="company_search_input"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            search_button = st.button("🔍 ค้นหา", type="primary", use_container_width=True, key="search_company_btn")
        
        with col2:
            if st.button("🧹 ล้างข้อมูล", use_container_width=True, key="clear_search_btn"):
                st.rerun()
        
        if search_button and company_name:
            # แสดงคำเตือนถ้าใช้ browser mode
            if use_browser_mode and not headless_mode:
                st.info("👀 **ดู Chromium Browser ที่เปิดอยู่** - จะเห็นการทำงานแบบเรียลไทม์!")
            
            with st.spinner("กำลังค้นหาข้อมูล..."):
                # แสดงขั้นตอนการทำงาน
                log_messages = []
                log_expander = st.expander("🔍 ดูขั้นตอนการทำงาน", expanded=True)
                
                def log_callback(message, status="info"):
                    log_messages.append({
                        "message": message,
                        "status": status,
                        "time": datetime.now().strftime("%H:%M:%S")
                    })
                    
                    # แสดง log
                    log_text = ""
                    for log in log_messages:
                        icon = {
                            "info": "ℹ️",
                            "success": "✅",
                            "warning": "⚠️",
                            "error": "❌"
                        }.get(log["status"], "📝")
                        log_text += f"[{log['time']}] {icon} {log['message']}\n"
                    
                    log_expander.code(log_text, language=None)
                
                # สร้าง bot instance
                try:
                    if use_browser_mode:
                        st.info("🚀 **กำลังเปิด Chromium Browser...**")
                        st.info("👀 **Browser จะเปิดขึ้นมาในอีกสักครู่ - รอสักครู่แล้วดู Browser window!**")
                    
                    bot = DBDDataWarehouseBot(use_browser=use_browser_mode, headless=headless_mode)
                    
                    if use_browser_mode and bot.browser:
                        st.success("✅ **Browser เปิดสำเร็จ!**")
                        st.info("👀 **ดู Browser window ที่เปิดอยู่ - จะเห็นการทำงานทุกขั้นตอนแบบเรียลไทม์!**")
                        st.markdown("---")
                except Exception as e:
                    st.error(f"❌ **ไม่สามารถเปิด Browser ได้:** {str(e)}")
                    st.warning("⚠️ **ระบบจะใช้ Requests Mode แทน** (อาจไม่ทำงานเพราะ JavaScript protection)")
                    st.info("💡 **วิธีแก้:**")
                    st.code("pip install playwright\nplaywright install chromium", language="bash")
                    # ยังคงดำเนินการต่อไปด้วย requests mode
                    bot = DBDDataWarehouseBot(use_browser=False, headless=False)
                
                # ค้นหาข้อมูล (พร้อม log callback)
                company_info = bot.search_company_info(company_name, log_callback=log_callback)
                
                if "error" in company_info:
                    st.error(f"❌ {company_info['error']}")
                else:
                    # แสดงผลลัพธ์ (ใช้โค้ดเดิมจากบรรทัด 1148-1418)
                    st.success("✅ พบข้อมูลบริษัท!")
                    st.markdown("---")
                    
                    # === ส่วนบน: ชื่อและเลขทะเบียน ===
                    header_col1, header_col2, header_col3 = st.columns([2, 2, 1])
                    with header_col1:
                        if company_info.get("company_name"):
                            st.markdown(f"**ชื่อนิติบุคคล:** {company_info.get('company_name')}")
                    with header_col2:
                        if company_info.get("registration_number"):
                            st.markdown(f"**เลขทะเบียนนิติบุคคล:** {company_info.get('registration_number')}")
                    with header_col3:
                        st.markdown(f"**ข้อมูล ณ วันที่:** {datetime.now().strftime('%d %b %y')}")
                    
                    st.markdown("---")
                    
                    def parse_card_values(raw_text: str) -> Tuple[str, str]:
                        """แปลงข้อความจาก card-infos ให้ได้ค่าประเภทธุรกิจและวัตถุประสงค์"""
                        if not raw_text:
                            return "", ""

                        lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
                        values = {"ประเภทธุรกิจ": "", "วัตถุประสงค์": ""}
                        current_label: Optional[str] = None

                        for line in lines:
                            normalized = line.replace(':', '').strip()

                            if normalized in values:
                                current_label = normalized
                                value = ""
                                if ':' in line:
                                    value = line.split(':', 1)[1].strip()

                                if value:
                                    values[normalized] = value
                                    current_label = None
                                else:
                                    # เตรียมรับข้อมูลจากบรรทัดถัดไป
                                    values.setdefault(normalized, "")
                                continue

                            if current_label:
                                if values[current_label]:
                                    values[current_label] += f" {line}"
                                else:
                                    values[current_label] = line

                        return values.get("ประเภทธุรกิจ", ""), values.get("วัตถุประสงค์", "")

                    # === ตารางที่ 1: ข้อมูลบริษัท (รวมทุกข้อมูล) ===
                    st.subheader("📋 ข้อมูลบริษัท")
                    
                    # สร้างตารางข้อมูลหลัก - เรียงลำดับตามตัวอย่าง
                    info_items = []
                    info_values = []
                    
                    # 1. ชื่อนิติบุคคล
                    if company_info.get("company_name"):
                        info_items.append("ชื่อนิติบุคคล")
                        info_values.append(str(company_info.get("company_name", "-")))
                    
                    # 2. เลขทะเบียนนิติบุคคล
                    if company_info.get("registration_number"):
                        info_items.append("เลขทะเบียนนิติบุคคล")
                        info_values.append(str(company_info.get("registration_number", "-")))
                    
                    # 3. ที่ตั้งสำนักงานแห่งใหญ่
                    if company_info.get("address"):
                        info_items.append("ที่ตั้งสำนักงานแห่งใหญ่")
                        address = str(company_info.get("address", "-"))
                        info_values.append(address.replace('\n', ' '))
                    
                    # 4. รายชื่อกรรมการ (รวมเป็นข้อความเดียว)
                    if company_info.get("directors"):
                        directors_text = company_info.get("directors", "").strip()
                        if directors_text:
                            directors_list = [d.strip() for d in directors_text.split('\n') if d.strip()]
                            # ข้ามหัวข้อถ้ามี
                            filtered_directors = []
                            for director in directors_list:
                                if 'รายชื่อกรรมการ' not in director:
                                    filtered_directors.append(director)
                            
                            if filtered_directors:
                                directors_str = " ".join([f"{i+1}. {d}" for i, d in enumerate(filtered_directors)])
                                info_items.append("รายชื่อกรรมการ")
                                info_values.append(directors_str)
                    
                    # 5. กรรมการลงชื่อผูกพัน
                    if company_info.get("authorized_signatories"):
                        auth_text = company_info.get("authorized_signatories", "").strip()
                        if auth_text:
                            if 'กรรมการลงชื่อผูกพัน' in auth_text:
                                auth_text = auth_text.replace('กรรมการลงชื่อผูกพัน', '').strip()
                            info_items.append("กรรมการลงชื่อผูกพัน")
                            info_values.append(auth_text)
                    
                    # 6. ประเภทนิติบุคคล
                    if company_info.get("business_type"):
                        info_items.append("ประเภทนิติบุคคล")
                        info_values.append(str(company_info.get("business_type", "-")))
                    
                    # 7. สถานะนิติบุคคล
                    if company_info.get("status"):
                        info_items.append("สถานะนิติบุคคล")
                        status = str(company_info.get("status", "-"))
                        info_values.append(status)
                    
                    # 8. วันที่จดทะเบียนจัดตั้ง
                    if company_info.get("found_date"):
                        info_items.append("วันที่จดทะเบียนจัดตั้ง")
                        info_values.append(str(company_info.get("found_date", "-")))
                    
                    # 9. ทุนจดทะเบียน
                    if company_info.get("registered_capital"):
                        info_items.append("ทุนจดทะเบียน")
                        info_values.append(str(company_info.get("registered_capital", "-")))
                    
                    # 10. เลขทะเบียนเดิม
                    if company_info.get("old_registration_number"):
                        info_items.append("เลขทะเบียนเดิม")
                        old_reg = str(company_info.get("old_registration_number", "-"))
                        info_values.append(old_reg if old_reg != "-" else "-")
                    
                    # 11. กลุ่มธุรกิจ
                    if company_info.get("business_group"):
                        info_items.append("กลุ่มธุรกิจ")
                        info_values.append(str(company_info.get("business_group", "-")))
                    
                    # 12. ขนาดธุรกิจ
                    if company_info.get("business_size"):
                        info_items.append("ขนาดธุรกิจ")
                        info_values.append(str(company_info.get("business_size", "-")))
                    
                    # 13. Website
                    if company_info.get("website"):
                        info_items.append("Website")
                        website = str(company_info.get("website", "-"))
                        if website.startswith('-'):
                            info_values.append(website)
                        elif website.startswith('http'):
                            info_values.append(f"- [{website}]({website})")
                        else:
                            info_values.append(f"- {website}")
                    
                    # แสดงตาราง
                    if info_items:
                        info_data = {
                            "รายการ": info_items,
                            "ข้อมูล": info_values
                        }
                        df_result = pd.DataFrame(info_data)
                        st.dataframe(df_result, use_container_width=True, hide_index=True)
                    else:
                        st.info("ไม่มีข้อมูลบริษัท")
                    
                    st.markdown("---")
                    
                    # === ตารางที่ 2: ประเภทธุรกิจตอนจดทะเบียน ===
                    st.subheader("🏢 ประเภทธุรกิจตอนจดทะเบียน")
                    
                    biz_reg_type = (company_info.get("business_type_registration") or "").strip()
                    biz_reg_objective = (company_info.get("business_type_registration_objective") or "").strip()

                    if not (biz_reg_type or biz_reg_objective):
                        raw_text = (company_info.get("business_type_registration_raw") or "").strip()
                        if raw_text:
                            parsed_type, parsed_objective = parse_card_values(raw_text)
                            biz_reg_type = parsed_type.strip()
                            biz_reg_objective = parsed_objective.strip()

                    if biz_reg_type or biz_reg_objective:
                        reg_data = {
                            "ประเภทธุรกิจ": [biz_reg_type if biz_reg_type else "-"],
                            "วัตถุประสงค์": [biz_reg_objective if biz_reg_objective else "-"]
                        }
                        df_biz_reg = pd.DataFrame(reg_data)
                        st.dataframe(df_biz_reg, use_container_width=True, hide_index=True)
                    else:
                        st.info("ไม่มีข้อมูลประเภทธุรกิจตอนจดทะเบียน")
                    
                    st.markdown("---")
                    
                    # === ตารางที่ 3: ประเภทธุรกิจที่ส่งงบการเงินปีล่าสุด ===
                    st.subheader("📊 ประเภทธุรกิจที่ส่งงบการเงินปีล่าสุด")
                    
                    biz_latest_type = (company_info.get("business_type_latest") or "").strip()
                    biz_latest_objective = (company_info.get("business_type_latest_objective") or "").strip()

                    if not (biz_latest_type or biz_latest_objective):
                        raw_latest_text = (company_info.get("business_type_latest_raw") or "").strip()
                        if raw_latest_text:
                            parsed_type, parsed_objective = parse_card_values(raw_latest_text)
                            biz_latest_type = parsed_type.strip()
                            biz_latest_objective = parsed_objective.strip()

                    if biz_latest_type or biz_latest_objective:
                        latest_data = {
                            "ประเภทธุรกิจ": [biz_latest_type if biz_latest_type else "-"],
                            "วัตถุประสงค์": [biz_latest_objective if biz_latest_objective else "-"]
                        }
                        df_biz_latest = pd.DataFrame(latest_data)
                        st.dataframe(df_biz_latest, use_container_width=True, hide_index=True)
                    else:
                        st.info("ไม่มีข้อมูลประเภทธุรกิจที่ส่งงบการเงินปีล่าสุด")
                    
                    # === ส่วนที่ 3: ข้อมูลละเอียดทั้งหมด (Expander) ===
                    if company_info.get("company_details"):
                        st.markdown("---")
                        with st.expander("📄 ข้อมูลนิติบุคคลทั้งหมด (คลิกเพื่อขยาย)", expanded=False):
                            # แยกและแสดงข้อมูลตามหัวข้อใหญ่
                            details_text = company_info.get("company_details", "").strip()
                            if details_text:
                                lines = details_text.split('\n')
                                current_section = None
                                
                                for line in lines:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    
                                    # หัวข้อหลัก (ไม่มี ":")
                                    if 'ข้อมูลนิติบุคคล' in line and ':' not in line:
                                        current_section = "company_info"
                                        st.markdown(f"### {line}")
                                        continue
                                    elif 'กลุ่มธุรกิจ' in line and ':' not in line:
                                        if current_section:
                                            st.markdown("---")
                                        current_section = "business_group"
                                        st.markdown(f"### {line}")
                                        continue
                                    elif 'ปีที่ส่งงบการเงิน' in line and ':' not in line:
                                        if current_section:
                                            st.markdown("---")
                                        current_section = "financial_years"
                                        st.markdown(f"### {line}")
                                        continue
                                    elif 'ที่ตั้งสำนักงานแห่งใหญ่' in line and ':' not in line:
                                        if current_section:
                                            st.markdown("---")
                                        current_section = "address"
                                        st.markdown(f"### {line}")
                                        continue
                                    elif 'Website' in line and ':' not in line:
                                        if current_section:
                                            st.markdown("---")
                                        current_section = "website"
                                        st.markdown(f"### {line}")
                                        continue
                                    
                                    # แสดงข้อมูล Key-Value
                                    if ':' in line:
                                        parts = line.split(':', 1)
                                        if len(parts) == 2:
                                            key = parts[0].strip()
                                            value = parts[1].strip()
                                            # ถ้าเป็น URL ให้แสดงเป็น link
                                            if value.startswith('http') or '://' in value:
                                                st.markdown(f"**{key}:** - [{value}]({value})")
                                            else:
                                                st.markdown(f"**{key}:** {value}")
                                        else:
                                            st.write(line)
                                    else:
                                        # ข้อความเพิ่มเติม (เช่น note)
                                        st.write(line)
                            else:
                                st.info("ไม่มีข้อมูลเพิ่มเติม")
    
    elif mode == "📊 อัปโหลดไฟล์ Excel":
        st.subheader("📊 อัปโหลดไฟล์ Excel")
        
        uploaded_excel_bot = st.file_uploader(
            "เลือกไฟล์ Excel",
            type=['xlsx', 'xls'],
            help="อัปโหลดไฟล์ Excel ที่มีคอลัมน์ชื่อบริษัท/บุคคล",
            key="excel_upload_bot"
        )
        
        if uploaded_excel_bot is not None:
            try:
                # อ่านไฟล์ Excel
                df_excel_bot = pd.read_excel(uploaded_excel_bot)
                
                st.success("✅ อัปโหลดไฟล์ Excel สำเร็จ!")
                
                # แสดงข้อมูลตัวอย่าง
                st.subheader("📊 ข้อมูลตัวอย่าง")
                st.write(f"**จำนวนแถว:** {len(df_excel_bot)}")
                st.write(f"**จำนวนคอลัมน์:** {len(df_excel_bot.columns)}")
                
                # แสดงคอลัมน์ที่มีอยู่
                st.write("**คอลัมน์ที่มีอยู่:**")
                st.write(", ".join(df_excel_bot.columns.tolist()))
                
                # เลือกคอลัมน์ที่มีชื่อบริษัท
                company_columns = [col for col in df_excel_bot.columns if any(keyword in col.lower() for keyword in ['บริษัท', 'ชื่อ', 'company', 'name'])]
                
                if company_columns:
                    selected_column_bot = st.selectbox(
                        "เลือกคอลัมน์ที่มีชื่อบริษัท/บุคคล:",
                        company_columns,
                        help="เลือกคอลัมน์ที่มีชื่อบริษัทหรือบุคคล",
                        key="company_column_bot"
                    )
                    
                    if selected_column_bot:
                        # แสดงข้อมูลตัวอย่าง
                        st.subheader("📋 ข้อมูลตัวอย่าง")
                        st.dataframe(df_excel_bot.head(10), use_container_width=True)
                        
                        # ปุ่มประมวลผล
                        if st.button("🚀 เริ่มประมวลผล", type="primary", use_container_width=True, key="process_excel_bot"):
                            # แสดงคำเตือนถ้าใช้ browser mode
                            if use_browser_mode and not headless_mode:
                                st.info("👀 **ดู Chromium Browser ที่เปิดอยู่** - จะเห็นการทำงานของทุกบริษัทแบบเรียลไทม์!")
                            
                            # ดึงข้อมูลจาก DBD
                            df_excel_with_dbd = integrate_with_streamlit(
                                df_excel_bot.copy(), 
                                selected_column_bot,
                                use_browser=use_browser_mode,
                                headless=headless_mode
                            )
                            
                            # แสดงตารางสรุปข้อมูล DBD
                            dbd_summary = create_dbd_summary_table(df_excel_with_dbd)
                            
                            if not dbd_summary.empty:
                                st.subheader("📋 สรุปข้อมูล DBD")
                                st.dataframe(dbd_summary, use_container_width=True)
                            
                            # สร้างไฟล์ Excel พร้อมข้อมูล DBD
                            output_dbd = io.BytesIO()
                            with pd.ExcelWriter(output_dbd, engine='openpyxl') as writer:
                                df_excel_with_dbd.to_excel(writer, sheet_name='ข้อมูลพร้อม DBD', index=False)
                                
                                if not dbd_summary.empty:
                                    dbd_summary.to_excel(writer, sheet_name='สรุปข้อมูล DBD', index=False)
                            
                            output_dbd.seek(0)
                            
                            st.download_button(
                                label="📥 ดาวน์โหลดข้อมูลพร้อม DBD",
                                data=output_dbd.getvalue(),
                                file_name=f"excel_with_dbd_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                help="ข้อมูล Excel พร้อมข้อมูลจาก DBD DataWarehouse",
                                key="download_dbd_bot"
                            )
                            
                            # แสดงข้อมูลที่ประมวลผลแล้ว
                            st.subheader("📊 ข้อมูลที่ประมวลผลแล้ว")
                            st.dataframe(df_excel_with_dbd, use_container_width=True)
                else:
                    st.warning("⚠️ ไม่พบคอลัมน์ที่มีชื่อบริษัท/บุคคล")
                    st.write("**คอลัมน์ที่มีอยู่:**")
                    for col in df_excel_bot.columns:
                        st.write(f"• {col}")
            
            except Exception as e:
                st.error(f"❌ เกิดข้อผิดพลาดในการอ่านไฟล์ Excel: {str(e)}")

def render_receipt_bot_page():
    """หน้า Bot รันเปิดใบเสร็จ - ยังไม่สร้างระบบ แค่ปุ่มไว้"""
    st.header("🧾 Bot รันเปิดใบเสร็จ")
    st.markdown("---")
    st.write("**ระบบกรอกข้อมูลใบเสร็จอัตโนมัติ**")
    st.info("🎯 ขั้นตอนที่ 1: นำเข้าไฟล์ Excel ที่มีข้อมูลจาก DBD เพื่อเตรียมใช้กรอกในระบบ PEAKEngine")

    uploaded_peak_excel = st.file_uploader(
        "📁 เลือกไฟล์ Excel ที่มีชีต 'ข้อมูลพร้อม DBD'",
        type=['xlsx', 'xls'],
        help="ไฟล์จะถูกประมวลผลจากชีต 'ข้อมูลพร้อม DBD' และ 'สรุปข้อมูล DBD' เพื่อแสดงรายละเอียดก่อนนำไปใช้งาน"
    )

    def normalize_registration_number(reg_value):
        if pd.isna(reg_value):
            return ""
        reg_str = str(reg_value).strip()
        if not reg_str:
            return ""
        digits = "".join(ch for ch in reg_str if ch.isdigit())
        if not digits:
            return ""
        digits = digits[-13:]
        if len(digits) < 13:
            digits = digits.zfill(13)
        if digits[0] != "0":
            digits = "0" + digits[1:]
        return digits

    def parse_dbd_info(text: str) -> Dict[str, str]:
        results = {}
        if not text or pd.isna(text):
            return results
        parts = [part.strip() for part in str(text).split('|')]
        for part in parts:
            if ':' not in part:
                continue
            key, value = part.split(':', 1)
            results[key.strip()] = value.strip()
        return results

    def slugify_filename(value: Any) -> str:
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none"}:
            return "type"
        sanitized = re.sub(r"[^\wก-๙\-]+", "_", text)
        sanitized = re.sub(r"_+", "_", sanitized).strip("_")
        return sanitized or "type"

    def build_summary_from_source(source_df: pd.DataFrame) -> pd.DataFrame:
        if source_df is None or source_df.empty:
            return pd.DataFrame()

        summary_rows: List[Dict[str, Any]] = []
        for _, row in source_df.iterrows():
            dbd_parsed = parse_dbd_info(row.get("ข้อมูล DBD", ""))
            reg_candidates = [
                row.get("เลขทะเบียน"),
                row.get("เลขทะเบียนจาก DBD"),
                row.get("เลขทะเบียนนิติบุคคล"),
                dbd_parsed.get("เลขทะเบียน")
            ]
            normalized_reg = ""
            for candidate in reg_candidates:
                normalized_reg = normalize_registration_number(candidate)
                if normalized_reg:
                    break
            if not normalized_reg:
                continue

            summary_row: Dict[str, Any] = {}
            columns_to_preserve = [
                "เลขทะเบียน",
                "เลขทะเบียนจาก DBD",
                "เลขทะเบียนนิติบุคคล",
                "ชื่อบริษัท/บุคคล",
                "ชื่อบริษัทจาก DBD",
                "ประเภทผู้ส่งโอน",
                "ข้อมูล DBD",
                "สถานะข้อมูล DBD",
                "ประเภทธุรกิจ",
                "สถานะกิจการ",
                "ทุนจดทะเบียน",
                "ที่อยู่",
                "ที่อยู่_บ้านเลขที่",
                "ที่อยู่_หมู่บ้าน",
                "ที่อยู่_หมู่ที่",
                "ที่อยู่_ตำบล",
                "ที่อยู่_อำเภอ",
                "ที่อยู่_จังหวัด",
                "ที่อยู่_รหัสไปรษณีย์"
            ]
            for column_name in columns_to_preserve:
                if column_name in row:
                    summary_row[column_name] = row.get(column_name, "")

            summary_row["เลขทะเบียน_พร้อมใช้งาน"] = normalized_reg
            summary_row["ประเภทธุรกิจจาก DBD"] = summary_row.get("ประเภทธุรกิจ", "") or dbd_parsed.get("ประเภทธุรกิจ", "")
            summary_row["สถานะจาก DBD"] = summary_row.get("สถานะกิจการ", "") or dbd_parsed.get("สถานะ", "")
            summary_row["ทุนจดทะเบียนจาก DBD"] = summary_row.get("ทุนจดทะเบียน", "") or dbd_parsed.get("ทุนจดทะเบียน", "")
            summary_row["ที่อยู่จาก DBD"] = summary_row.get("ที่อยู่", "") or dbd_parsed.get("ที่อยู่", "")

            summary_rows.append(summary_row)

        if not summary_rows:
            return pd.DataFrame()

        summary_df = pd.DataFrame(summary_rows)
        return summary_df

    if uploaded_peak_excel is not None:
        with st.spinner("กำลังตรวจสอบไฟล์ Excel..."):
            try:
                excel_file = pd.ExcelFile(uploaded_peak_excel)
                if "ข้อมูลพร้อม DBD" not in excel_file.sheet_names:
                    available_sheets = ", ".join(excel_file.sheet_names)
                    st.error("❌ ไม่พบชีต 'ข้อมูลพร้อม DBD' ในไฟล์ที่อัปโหลด")
                    if available_sheets:
                        st.info(f"📄 ชีตที่พบ: {available_sheets}")
                else:
                    df_peak = pd.read_excel(excel_file, sheet_name="ข้อมูลพร้อม DBD")
                    st.success("✅ โหลดข้อมูลจากชีต 'ข้อมูลพร้อม DBD' สำเร็จ!")

                    available_company_cols = [col for col in df_peak.columns if "ชื่อบริษัท" in str(col)]
                    transfer_type_col = next((col for col in df_peak.columns if "ประเภทผู้ส่งโอน" in str(col)), None)

                    df_peak_filtered = df_peak.copy()
                    selected_transfer_types_state: List[str] = st.session_state.get("peakengine_selected_transfer_types", [])

                    if transfer_type_col:
                        st.subheader("🎯 เลือกประเภทผู้ส่งโอนสำหรับประมวลผล")
                        transfer_series = df_peak[transfer_type_col].astype(str).str.strip()
                        transfer_series = transfer_series[~transfer_series.str.lower().eq("nan")]
                        unique_transfer_types = sorted([value for value in transfer_series.unique().tolist() if value])

                        if unique_transfer_types:
                            default_selection = [value for value in selected_transfer_types_state if value in unique_transfer_types]
                            if not default_selection:
                                default_selection = unique_transfer_types

                            selected_transfer_types = st.multiselect(
                                "เลือกประเภทผู้ส่งโอนที่จะให้ระบบทำงาน:",
                                unique_transfer_types,
                                default=default_selection,
                                help="เลือกประเภทผู้ส่งโอนที่ต้องการให้ระบบทำงานเฉพาะรายการนั้นๆ",
                                key="peakengine_transfer_type_multiselect"
                            )

                            st.session_state["peakengine_selected_transfer_types"] = selected_transfer_types

                            if selected_transfer_types:
                                filtered_mask = transfer_series.isin(selected_transfer_types)
                                df_peak_filtered = df_peak[filtered_mask].copy()
                                st.info(
                                    f"📌 เลือก {len(selected_transfer_types)} ประเภทผู้ส่งโอน • รายการที่ใช้ประมวลผล {len(df_peak_filtered)} จาก {len(df_peak)} แถว",
                                    icon="📊"
                                )
                            else:
                                df_peak_filtered = df_peak.iloc[0:0].copy()
                                st.warning("⚠️ ยังไม่ได้เลือกประเภทผู้ส่งโอน ระบบจะยังไม่ประมวลผลข้อมูลจากไฟล์นี้")
                        else:
                            st.info("ℹ️ ไม่พบค่าประเภทผู้ส่งโอนในไฟล์ Excel นี้", icon="ℹ️")
                            st.session_state["peakengine_selected_transfer_types"] = []
                    else:
                        st.info("ℹ️ ไม่พบคอลัมน์ 'ประเภทผู้ส่งโอน' ในไฟล์นี้ จึงไม่สามารถเลือกแยกการทำงานได้", icon="ℹ️")
                        st.session_state["peakengine_selected_transfer_types"] = []

                    st.session_state["peakengine_transfer_type_column"] = transfer_type_col

                    st.subheader("📊 สรุปข้อมูลจากไฟล์")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("จำนวนแถวทั้งหมด", len(df_peak))
                    with col2:
                        st.metric("จำนวนแถวที่เลือก", len(df_peak_filtered))
                    with col3:
                        st.metric("คอลัมน์ชื่อบริษัทที่พบ", len(available_company_cols))

                    st.write("**คอลัมน์ทั้งหมดในชีต:**")
                    st.write(", ".join(df_peak.columns.astype(str).tolist()) or "-")

                    st.subheader("🔍 ตัวอย่างข้อมูล (5 แถวแรก)")
                    if df_peak_filtered.empty:
                        st.warning("⚠️ ไม่มีข้อมูลหลังจากใช้งานตัวกรองประเภทผู้ส่งโอน")
                    else:
                        st.dataframe(df_peak_filtered.head(5), use_container_width=True)

                    if available_company_cols:
                        st.caption(f"🎯 จะใช้คอลัมน์เหล่านี้ในการกรอกข้อมูล: {', '.join(available_company_cols)}")
                    else:
                        st.warning("⚠️ ไม่พบคอลัมน์ที่มีคำว่า 'ชื่อบริษัท' ภายในหัวคอลัมน์ โปรดตรวจสอบไฟล์")

                    # เก็บข้อมูลไว้ใน session state สำหรับขั้นตอนถัดไป
                    def classify_dbd_status(value: Any) -> str:
                        if value is None:
                            return "ค่าว่าง"
                        if isinstance(value, float) and pd.isna(value):
                            return "ค่าว่าง"
                        text = str(value).strip()
                        lower_text = text.lower()
                        if not text or lower_text in {"nan", "none", ""}:
                            return "ค่าว่าง"
                        if "ไม่พบ" in lower_text or "ไม่มีข้อมูล" in lower_text:
                            return "ไม่มีข้อมูล"
                        if "ข้อผิดพลาด" in lower_text or "error" in lower_text:
                            return "ข้อผิดพลาด"
                        if "มีข้อมูล" in lower_text or "พบข้อมูล" in lower_text:
                            return "มีข้อมูล"
                        return "มีข้อมูล"

                    if not df_peak_filtered.empty:
                        df_peak_filtered = df_peak_filtered.copy()
                        df_peak_filtered.loc[:, "สถานะข้อมูล DBD"] = df_peak_filtered["ข้อมูล DBD"].apply(classify_dbd_status)
                        status_counts = df_peak_filtered["สถานะข้อมูล DBD"].value_counts()
                        st.subheader("📊 สถานะข้อมูล DBD")
                        col_status = st.columns(len(status_counts) if status_counts.size else 1)
                        for idx, (status_label, count_value) in enumerate(status_counts.items()):
                            with col_status[idx]:
                                st.metric(status_label, count_value)

                        with st.expander("🗂️ ดูข้อมูลตามสถานะ", expanded=False):
                            available_statuses = sorted(df_peak_filtered["สถานะข้อมูล DBD"].unique().tolist())
                            default_status_filter = st.session_state.get("peakengine_status_filter", available_statuses)
                            default_status_filter = [status for status in default_status_filter if status in available_statuses]
                            if not default_status_filter:
                                default_status_filter = available_statuses

                            status_filter = st.multiselect(
                                "เลือกสถานะข้อมูล DBD ที่ต้องการแสดง",
                                options=available_statuses,
                                default=default_status_filter,
                                help="กรองข้อมูลตามสถานะจากคอลัมน์ 'ข้อมูล DBD'",
                                key="peakengine_status_filter"
                            )

                            search_keyword = st.text_input(
                                "ค้นหาชื่อบริษัท/คำสำคัญ",
                                value=st.session_state.get("peakengine_status_search", ""),
                                help="พิมพ์คำค้นหาที่ต้องการ เช่น ชื่อบริษัท เลขทะเบียน หรือคำอื่นๆ",
                                key="peakengine_status_search"
                            )

                            max_rows = st.number_input(
                                "จำนวนแถวที่ต้องการแสดง",
                                min_value=10,
                                max_value=max(10, len(df_peak_filtered)),
                                value=min(100, max(10, len(df_peak_filtered))),
                                step=10,
                                help="จำกัดจำนวนแถวที่แสดงเพื่อความสะดวกในการตรวจสอบ"
                            )

                            filtered_view = df_peak_filtered.copy()
                            if status_filter:
                                filtered_view = filtered_view[filtered_view["สถานะข้อมูล DBD"].isin(status_filter)]

                            if search_keyword:
                                keyword = search_keyword.strip().lower()
                                if keyword:
                                    mask = (
                                        filtered_view["ข้อมูล DBD"].astype(str).str.lower().str.contains(keyword, na=False)
                                        | filtered_view["ชื่อบริษัทจาก DBD"].astype(str).str.lower().str.contains(keyword, na=False)
                                        | filtered_view["ชื่อบริษัท/บุคคล"].astype(str).str.lower().str.contains(keyword, na=False)
                                    )
                                    filtered_view = filtered_view[mask]

                            st.write(f"พบ {len(filtered_view)} รายการ (จากทั้งหมด {len(df_peak_filtered)})")

                            if filtered_view.empty:
                                st.info("ไม่พบข้อมูลตามเงื่อนไขที่เลือก")
                            else:
                                st.dataframe(
                                    filtered_view.head(int(max_rows)),
                                    use_container_width=True
                                )

                    st.session_state["peakengine_source_df_full"] = df_peak
                    st.session_state["peakengine_source_df"] = df_peak_filtered
                    st.session_state["peakengine_source_filename"] = getattr(uploaded_peak_excel, "name", "uploaded.xlsx")

                    if transfer_type_col and not df_peak.empty:
                        st.subheader("💾 ดาวน์โหลดข้อมูลตามประเภทผู้ส่งโอน (ข้อมูลพร้อม DBD)")
                        st.caption("รวมทุกแถวจากชีต 'ข้อมูลพร้อม DBD' ของประเภทที่เลือกไว้ พร้อมข้อมูล DBD ครบถ้วน (รวมรายการที่ยอดไม่เป็นบวกหรือไม่พบข้อมูล)")

                        transfer_series_full = df_peak[transfer_type_col].astype(str).str.strip()
                        transfer_series_full = transfer_series_full[~transfer_series_full.str.lower().eq("nan")]
                        available_export_types = sorted([value for value in transfer_series_full.unique().tolist() if value])

                        if available_export_types:
                            export_datasets_step1: List[Tuple[str, bytes, str]] = []
                            timestamp_step1 = datetime.now().strftime("%Y%m%d_%H%M%S")
                            zip_buffer_step1 = io.BytesIO()

                            with zipfile.ZipFile(zip_buffer_step1, "w", zipfile.ZIP_DEFLATED) as zip_file:
                                for type_value in available_export_types:
                                    type_mask_full = transfer_series_full == type_value
                                    type_df_full = df_peak[type_mask_full].copy()
                                    if type_df_full.empty:
                                        continue

                                    excel_buffer = io.BytesIO()
                                    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                                        type_df_full.to_excel(writer, sheet_name="ข้อมูลพร้อม DBD", index=False)
                                    excel_buffer.seek(0)

                                    safe_type_name = slugify_filename(type_value)
                                    excel_filename = f"ข้อมูลพร้อมDBD_{safe_type_name}_{timestamp_step1}.xlsx"

                                    zip_file.writestr(excel_filename, excel_buffer.getvalue())
                                    export_datasets_step1.append((type_value, excel_buffer.getvalue(), excel_filename))

                            if export_datasets_step1:
                                zip_buffer_step1.seek(0)
                                st.download_button(
                                    label="📦 ดาวน์โหลดทุกประเภทเป็น ZIP (ข้อมูลพร้อม DBD)",
                                    data=zip_buffer_step1.getvalue(),
                                    file_name=f"ข้อมูลพร้อมDBD_ตามประเภท_{timestamp_step1}.zip",
                                    mime="application/zip",
                                    key="download_step1_zip_all"
                                )

                                st.caption("หรือดาวน์โหลดเฉพาะประเภทที่สนใจ:")
                                num_cols_step1 = min(3, len(export_datasets_step1))
                                cols_step1 = st.columns(num_cols_step1)

                                for idx_export, (type_value, excel_bytes, excel_filename) in enumerate(export_datasets_step1):
                                    target_col = cols_step1[idx_export % num_cols_step1]
                                    with target_col:
                                        st.download_button(
                                            label=f"📥 ดาวน์โหลด {type_value}",
                                            data=excel_bytes,
                                            file_name=excel_filename,
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key=f"download_step1_{slugify_filename(f'{type_value}_{idx_export}')}"
                                        )
                        else:
                            st.info("ไม่พบข้อมูลประเภทผู้ส่งโอนสำหรับดาวน์โหลด", icon="ℹ️")

                    reg_info_map: Dict[str, Dict[str, Any]] = {}
                    for idx_row, row in df_peak_filtered.iterrows():
                        dbd_raw = row.get("ข้อมูล DBD", "")
                        dbd_parsed = parse_dbd_info(dbd_raw)
                        reg_candidate = (
                            row.get("เลขทะเบียน")
                            or row.get("เลขทะเบียนจาก DBD")
                            or row.get("เลขทะเบียนนิติบุคคล")
                            or dbd_parsed.get("เลขทะเบียน")
                        )
                        reg_normalized = normalize_registration_number(reg_candidate)
                        if not reg_normalized:
                            continue
                        row_dict = row.to_dict()
                        reg_info_map[reg_normalized] = {
                            "registration": reg_normalized,
                            "dbd_raw": dbd_raw,
                            "dbd_info": dbd_parsed,
                            "transfer_type": str(row.get("ประเภทผู้ส่งโอน", "")).strip(),
                            "dbd_status": row.get("สถานะข้อมูล DBD", ""),
                            "company_name": row.get("ชื่อบริษัทจาก DBD", ""),
                            "row_index": int(idx_row),
                            "row": row_dict
                        }
                    st.session_state["peakengine_reg_info_map"] = reg_info_map

                    summary_df: Optional[pd.DataFrame] = None
                    summary_title = "📑 สรุปข้อมูล DBD"
                    summary_caption = ""

                    if "สรุปข้อมูล DBD" in excel_file.sheet_names:
                        try:
                            df_summary = pd.read_excel(excel_file, sheet_name="สรุปข้อมูล DBD")

                            if "เลขทะเบียน" in df_summary.columns:
                                df_summary["เลขทะเบียน_พร้อมใช้งาน"] = df_summary["เลขทะเบียน"].apply(normalize_registration_number)
                            else:
                                df_summary["เลขทะเบียน_พร้อมใช้งาน"] = ""

                            selected_transfer_types_summary = st.session_state.get("peakengine_selected_transfer_types", [])
                            if transfer_type_col and selected_transfer_types_summary and transfer_type_col in df_summary.columns:
                                summary_type_series = df_summary[transfer_type_col].astype(str).str.strip()
                                summary_mask = summary_type_series.isin(selected_transfer_types_summary)
                                df_summary = df_summary[summary_mask].copy()
                                if df_summary.empty:
                                    st.warning("⚠️ ไม่มีข้อมูลในชีต 'สรุปข้อมูล DBD' ที่ตรงกับประเภทผู้ส่งโอนที่เลือก")

                            summary_df = df_summary if not df_summary.empty else None
                        except Exception as summary_error:
                            st.warning(f"⚠️ อ่านชีต 'สรุปข้อมูล DBD' ไม่สำเร็จ: {summary_error}")

                    if summary_df is None or summary_df.empty:
                        summary_df = build_summary_from_source(df_peak_filtered)
                        summary_title = "📑 สรุปเลขทะเบียนจากข้อมูลพร้อม DBD"
                        summary_caption = "⚠️ ใช้ข้อมูลจากชีต 'ข้อมูลพร้อม DBD' แทน เนื่องจากไม่พบหรือไม่สามารถใช้ชีต 'สรุปข้อมูล DBD'"

                    if summary_df is not None and not summary_df.empty:
                        st.session_state["peakengine_summary_df"] = summary_df
                        st.subheader(summary_title)
                        if summary_caption:
                            st.caption(summary_caption)

                        reg_series = summary_df.get("เลขทะเบียน_พร้อมใช้งาน", summary_df.get("เลขทะเบียน"))
                        valid_reg = []
                        if reg_series is not None:
                            reg_series = reg_series.astype(str).str.strip()
                            valid_reg = [reg for reg in reg_series if reg and reg.lower() != "nan"]

                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("จำนวนแถวในสรุป", len(summary_df))
                        with col2:
                            st.metric("จำนวนเลขทะเบียนที่ไม่ว่าง", len(valid_reg))
                        with col3:
                            unique_regs = list(dict.fromkeys(valid_reg))
                            st.metric("เลขทะเบียนไม่ซ้ำ", len(unique_regs))

                        st.write("**คอลัมน์ในข้อมูลสรุป:**")
                        st.write(", ".join(summary_df.columns.astype(str).tolist()) or "-")

                        st.subheader("🔍 ตัวอย่างข้อมูลสรุป (5 แถวแรก)")
                        st.dataframe(summary_df.head(5), use_container_width=True)
                    else:
                        st.error("❌ ไม่พบเลขทะเบียนที่สามารถใช้งานได้จากไฟล์นี้")
                        st.session_state["peakengine_summary_df"] = pd.DataFrame()

            except Exception as e:
                st.error(f"❌ เกิดข้อผิดพลาดในการอ่านไฟล์ Excel: {str(e)}")
    else:
        st.info("📥 กรุณาอัปโหลดไฟล์ Excel เพื่อเริ่มตั้งค่าขั้นตอนการกรอกข้อมูล")

    summary_df = st.session_state.get("peakengine_summary_df")
    if summary_df is not None and not summary_df.empty:
        st.markdown("---")
        st.subheader("🧾 ขั้นตอนที่ 2: เตรียมกรอกเลขทะเบียนใน PEAKEngine")

        reg_series = summary_df.get("เลขทะเบียน_พร้อมใช้งาน", summary_df.get("เลขทะเบียน"))
        if reg_series is None:
            st.warning("⚠️ ไม่พบคอลัมน์ 'เลขทะเบียน' ในชีต 'สรุปข้อมูล DBD'")
            return

        reg_series = reg_series.astype(str).str.strip()
        registration_numbers = [reg for reg in reg_series if reg and reg.lower() != "nan"]
        unique_registration_numbers = list(dict.fromkeys(registration_numbers))

        df_source_for_filters = st.session_state.get("peakengine_source_df", pd.DataFrame())

        def normalize_status_value(raw_status: Any) -> str:
            text = str(raw_status).strip()
            lower_text = text.lower()
            if not text or lower_text in {"nan", "none", ""}:
                return "ค่าว่าง"
            if "ไม่พบ" in lower_text or "ไม่มีข้อมูล" in lower_text:
                return "ไม่มีข้อมูล"
            if "ข้อผิดพลาด" in lower_text or "error" in lower_text:
                return "ข้อผิดพลาด"
            if "มีข้อมูล" in lower_text or "พบข้อมูล" in lower_text:
                return "มีข้อมูล"
            return text

        def normalize_transfer_type(raw_type: Any) -> str:
            text = str(raw_type).strip()
            lower_text = text.lower()
            if not text or lower_text in {"nan", "none", "-", ""}:
                return "-"
            replacements = {
                "บริษัท(บจก.)": "บริษัท (บจก.)",
                "บริษัท(บจก)": "บริษัท (บจก.)",
                "บริษัท (บจก)": "บริษัท (บจก.)",
                "บริษัท จำกัด": "บริษัท (บจก.)",
                "บริษัท": "บริษัท (บจก.)",
                "บุคคล": "บุคคล",
                "หจก.": "ห้างหุ้นส่วน (หจก.)",
                "ห้างหุ้นส่วนจำกัด": "ห้างหุ้นส่วน (หจก.)",
                "ห้างหุ้นส่วน": "ห้างหุ้นส่วน (หจก.)"
            }
            normalized = replacements.get(text, replacements.get(lower_text, text))
            return normalized

        def format_amount_display(raw_amount: Any, fallback_numeric: Optional[float]) -> str:
            if isinstance(raw_amount, (int, float)) and not pd.isna(raw_amount):
                return f"{float(raw_amount):,.2f}"
            if isinstance(raw_amount, str):
                amount_str = raw_amount.strip()
                if amount_str.lower() in {"nan", "none", ""}:
                    pass
                else:
                    return amount_str
            if fallback_numeric is not None:
                return f"{fallback_numeric:,.2f}"
            return "-"

        def format_date_display(raw_date: Any) -> str:
            if raw_date is None:
                return "-"
            if isinstance(raw_date, pd.Timestamp):
                try:
                    return raw_date.to_pydatetime().strftime("%d/%m/%Y")
                except Exception:
                    return raw_date.strftime("%d/%m/%Y")
            if isinstance(raw_date, datetime):
                return raw_date.strftime("%d/%m/%Y")
            if isinstance(raw_date, (int, float)):
                try:
                    if pd.isna(raw_date):
                        return "-"
                except Exception:
                    pass
                try:
                    base_date = datetime(1899, 12, 30)
                    converted = base_date + timedelta(days=float(raw_date))
                    return converted.strftime("%d/%m/%Y")
                except Exception:
                    pass
            text = str(raw_date).strip()
            if not text or text.lower() in {"nan", "none", "-", "--"}:
                return "-"
            normalized_text = text.replace("T", " ").split("+", 1)[0].strip()
            date_formats = [
                "%d/%m/%Y",
                "%d-%m-%Y",
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y %H:%M",
                "%d-%m-%Y %H:%M:%S",
                "%d-%m-%Y %H:%M",
                "%d %b %Y",
                "%d %b %y"
            ]
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(normalized_text, fmt)
                    return dt.strftime("%d/%m/%Y")
                except Exception:
                    continue
            if " " in normalized_text:
                primary = normalized_text.split(" ", 1)[0].strip()
                if primary and primary != normalized_text:
                    formatted_primary = format_date_display(primary)
                    if formatted_primary and formatted_primary != "-":
                        return formatted_primary
            return normalized_text

        def get_row_amount(row_data: pd.Series) -> Optional[float]:
            amount_value = row_data.get("ยอดเงิน_numeric")
            if amount_value is not None and not (isinstance(amount_value, float) and pd.isna(amount_value)):
                try:
                    return float(amount_value)
                except (TypeError, ValueError):
                    pass

            raw_amount = row_data.get("จำนวนเงิน")
            if raw_amount is None:
                return None

            text_amount = str(raw_amount).strip()
            if not text_amount or text_amount.lower() in {"nan", "none"}:
                return None

            negative = False
            if text_amount.startswith("(") and text_amount.endswith(")"):
                negative = True
                text_amount = text_amount[1:-1]

            text_amount = text_amount.replace(",", "").replace("+", "").strip()
            if not text_amount:
                return None

            try:
                value = float(text_amount)
                return -value if negative else value
            except ValueError:
                return None

        def normalize_company_key(raw_name: Any) -> str:
            if raw_name is None:
                return ""
            text = str(raw_name).strip().lower()
            if not text or text in {"nan", "none", ""}:
                return ""
            replacements = [
                "บริษัทจำกัด", "บริษัท จำกัด", "บริษัท(จำกัด)", "บริษัท (จำกัด)",
                "บริษัท(บจก.)", "บริษัท (บจก.)", "บริษัทจำกัด (มหาชน)", "บริษัทมหาชนจำกัด",
                "บริษัท", "บจก.", "บจก", "จำกัด", "มหาชน",
                "ห้างหุ้นส่วนจำกัด", "ห้างหุ้นส่วน", "หจก.", "หจก"
            ]
            for phrase in replacements:
                text = text.replace(phrase.lower(), "")
            text = re.sub(r"[^\wก-๙]+", "", text)
            return text

        def clean_text(value: Any) -> str:
            if value is None:
                return ""
            text = str(value).strip()
            if not text or text.lower() in {"nan", "none"}:
                return ""
            return text

        def pick_first_text(*values: Any) -> str:
            for value in values:
                text = clean_text(value)
                if text:
                    return text
            return ""

        summary_df_for_lookup = st.session_state.get("peakengine_summary_df")
        summary_lookup: Dict[str, Dict[str, Any]] = {}
        if summary_df_for_lookup is not None and not summary_df_for_lookup.empty:
            for _, summary_row in summary_df_for_lookup.iterrows():
                reg_candidates = [
                    summary_row.get("เลขทะเบียน_พร้อมใช้งาน"),
                    summary_row.get("เลขทะเบียน")
                ]
                normalized_reg = ""
                for candidate in reg_candidates:
                    normalized_reg = normalize_registration_number(candidate)
                    if normalized_reg:
                        break
                if not normalized_reg:
                    continue
                summary_data_dict = summary_row.to_dict()
                potential_names = []
                for col_name in [
                    "ชื่อบริษัทจาก DBD",
                    "ชื่อบริษัท",
                    "ชื่อบริษัท/บุคคล",
                    "ชื่อบริษัท (ไฟล์ต้นทาง)",
                    "CompanyName"
                ]:
                    if col_name in summary_row and pd.notna(summary_row[col_name]):
                        potential_names.append(summary_row[col_name])
                for name in potential_names:
                    key = normalize_company_key(name)
                    if key and key not in summary_lookup:
                        summary_lookup[key] = {
                            "registration": normalized_reg,
                            "data": summary_data_dict
                        }

        positive_rows: List[Dict[str, Any]] = []
        if df_source_for_filters is not None and not df_source_for_filters.empty:
            for idx, row in df_source_for_filters.iterrows():
                amount_numeric = get_row_amount(row)
                if amount_numeric is None or amount_numeric <= 0:
                    continue

                dbd_info_raw_value = clean_text(row.get("ข้อมูล DBD", "")) or str(row.get("ข้อมูล DBD", "")).strip()
                dbd_info_dict = parse_dbd_info(dbd_info_raw_value)

                reg_candidate = (
                    row.get("เลขทะเบียน")
                    or row.get("เลขทะเบียนจาก DBD")
                    or row.get("เลขทะเบียนนิติบุคคล")
                    or dbd_info_dict.get("เลขทะเบียน")
                )
                reg_norm = normalize_registration_number(reg_candidate)

                status_value = normalize_status_value(row.get("สถานะข้อมูล DBD", ""))

                transfer_type_value = normalize_transfer_type(row.get("ประเภทผู้ส่งโอน", ""))

                company_value_raw = (
                    row.get("ชื่อบริษัทจาก DBD")
                    or row.get("ชื่อบริษัท/บุคคล")
                    or row.get("ชื่อบริษัท")
                    or ""
                )
                company_value = str(company_value_raw).strip() or "-"

                work_category_raw = row.get("ประเภทการทำงาน")
                work_category_value = clean_text(work_category_raw) or "-"
                normalized_work_category = work_category_value.strip().lower()
                skip_work_categories = {"", "-", "ไม่มีประเภทงาน", "เปิดบิลแล้ว", "เปิดบิลเอง", "บอทไม่ทำงาน"}
                valid_work_categories = {"ภาษีปกติ", "หัก ณ ที่จ่าย"}

                if normalized_work_category in skip_work_categories:
                    continue
                if normalized_work_category and normalized_work_category not in valid_work_categories:
                    st.warning(
                        f"⚠️ พบประเภทการทำงานที่ไม่รองรับ: '{work_category_value}' (แถวที่ {idx + 2}) รายการนี้จะถูกข้าม",
                        icon="⚠️"
                    )
                    continue

                raw_date_value = row.get("วันที่")
                date_value = format_date_display(raw_date_value)

                amount_display = format_amount_display(row.get("จำนวนเงิน"), amount_numeric)

                description_value_raw = str(row.get("คำอธิบาย", "")).strip()
                description_value = description_value_raw if description_value_raw.lower() not in {"nan", "none", ""} else "-"

                # DBD detail fields
                dbd_company_name = pick_first_text(
                    row.get("ชื่อบริษัทจาก DBD"),
                    dbd_info_dict.get("ชื่อบริษัท"),
                    dbd_info_dict.get("ชื่อกิจการ")
                )
                dbd_registration_text = pick_first_text(
                    row.get("เลขทะเบียนจาก DBD"),
                    row.get("เลขทะเบียน"),
                    row.get("เลขทะเบียนนิติบุคคล"),
                    dbd_info_dict.get("เลขทะเบียน")
                )
                dbd_business_type = pick_first_text(
                    row.get("ประเภทธุรกิจจาก DBD"),
                    row.get("ประเภทธุรกิจ"),
                    dbd_info_dict.get("ประเภทธุรกิจ")
                )
                dbd_status_detail = pick_first_text(
                    row.get("สถานะกิจการจาก DBD"),
                    row.get("สถานะกิจการ"),
                    dbd_info_dict.get("สถานะ")
                )
                dbd_capital = pick_first_text(
                    row.get("ทุนจดทะเบียนจาก DBD"),
                    row.get("ทุนจดทะเบียน"),
                    dbd_info_dict.get("ทุนจดทะเบียน")
                )
                dbd_address_text = pick_first_text(
                    row.get("ที่อยู่จาก DBD"),
                    dbd_info_dict.get("ที่อยู่")
                )
                dbd_address_house_no = clean_text(row.get("ที่อยู่_บ้านเลขที่"))
                dbd_address_village = clean_text(row.get("ที่อยู่_หมู่บ้าน"))
                dbd_address_moo = clean_text(row.get("ที่อยู่_หมู่ที่"))
                dbd_address_subdistrict = clean_text(row.get("ที่อยู่_ตำบล"))
                dbd_address_district = clean_text(row.get("ที่อยู่_อำเภอ"))
                dbd_address_province = clean_text(row.get("ที่อยู่_จังหวัด"))
                dbd_address_postal_code = clean_text(row.get("ที่อยู่_รหัสไปรษณีย์"))

                summary_match: Optional[Dict[str, Any]] = None
                if transfer_type_value in {"บริษัท (บจก.)", "ห้างหุ้นส่วน (หจก.)"} and summary_lookup:
                    candidate_names = [
                        row.get("ชื่อบริษัทจาก DBD", ""),
                        row.get("ชื่อบริษัท/บุคคล", ""),
                        row.get("ชื่อบริษัท", ""),
                        company_value,
                        description_value
                    ]
                    for candidate_name in candidate_names:
                        key = normalize_company_key(candidate_name)
                        if key and key in summary_lookup:
                            summary_match = summary_lookup[key]
                            break

                if summary_match:
                    summary_data = summary_match["data"]
                    if not reg_norm:
                        reg_from_summary = summary_match.get("registration")
                        reg_norm = normalize_registration_number(reg_from_summary) or reg_norm

                    dbd_company_name = dbd_company_name or pick_first_text(
                        summary_data.get("ชื่อบริษัทจาก DBD"),
                        summary_data.get("ชื่อบริษัท")
                    )
                    dbd_registration_text = dbd_registration_text or pick_first_text(
                        summary_data.get("เลขทะเบียน_พร้อมใช้งาน"),
                        summary_data.get("เลขทะเบียน")
                    )
                    dbd_business_type = dbd_business_type or pick_first_text(summary_data.get("ประเภทธุรกิจ"))
                    dbd_status_detail = dbd_status_detail or pick_first_text(summary_data.get("สถานะ"))
                    dbd_capital = dbd_capital or pick_first_text(summary_data.get("ทุนจดทะเบียน"))
                    dbd_address_text = dbd_address_text or pick_first_text(summary_data.get("ที่อยู่"))
                    dbd_address_house_no = dbd_address_house_no or clean_text(summary_data.get("ที่อยู่_บ้านเลขที่"))
                    dbd_address_village = dbd_address_village or clean_text(summary_data.get("ที่อยู่_หมู่บ้าน"))
                    dbd_address_moo = dbd_address_moo or clean_text(summary_data.get("ที่อยู่_หมู่ที่"))
                    dbd_address_subdistrict = dbd_address_subdistrict or clean_text(summary_data.get("ที่อยู่_ตำบล"))
                    dbd_address_district = dbd_address_district or clean_text(summary_data.get("ที่อยู่_อำเภอ"))
                    dbd_address_province = dbd_address_province or clean_text(summary_data.get("ที่อยู่_จังหวัด"))
                    dbd_address_postal_code = dbd_address_postal_code or clean_text(summary_data.get("ที่อยู่_รหัสไปรษณีย์"))

                dbd_registration_numeric = normalize_registration_number(dbd_registration_text)
                if not reg_norm:
                    reg_norm = dbd_registration_numeric or reg_norm
                if not reg_norm:
                    continue

                dbd_registration_display = pick_first_text(
                    dbd_registration_text,
                    dbd_info_dict.get("เลขทะเบียน"),
                    row.get("เลขทะเบียนจาก DBD"),
                    row.get("เลขทะเบียน"),
                    row.get("เลขทะเบียนนิติบุคคล")
                ) or "-"

                row_unique_key = f"{idx}_{len(positive_rows)}"
                positive_rows.append({
                    "row_key": row_unique_key,
                    "registration": reg_norm,
                    "dbd_status": status_value,
                    "transfer_type": transfer_type_value or "-",
                    "company_name": company_value or "-",
                    "document_date_raw": raw_date_value,
                    "work_category": work_category_value,
                    "date": date_value,
                    "amount": amount_display,
                    "amount_numeric": amount_numeric,
                    "description": description_value,
                    "dbd_info_raw": dbd_info_raw_value or "-",
                    "dbd_company_name": dbd_company_name or "-",
                    "dbd_registration": dbd_registration_display,
                    "dbd_business_type": dbd_business_type or "-",
                    "dbd_status_detail": dbd_status_detail or "-",
                    "dbd_capital": dbd_capital or "-",
                    "dbd_address_text": dbd_address_text or "-",
                    "dbd_address_house_no": dbd_address_house_no or "-",
                    "dbd_address_village": dbd_address_village or "-",
                    "dbd_address_moo": dbd_address_moo or "-",
                    "dbd_address_subdistrict": dbd_address_subdistrict or "-",
                    "dbd_address_district": dbd_address_district or "-",
                    "dbd_address_province": dbd_address_province or "-",
                    "dbd_address_postal_code": dbd_address_postal_code or "-",
                    "source_index": idx
                })

        if not positive_rows:
            st.warning("⚠️ ไม่มีเลขทะเบียนที่มียอดเงินเป็นบวกพร้อมสำหรับขั้นตอนที่ 2")
            return

        df_positive_rows = pd.DataFrame(positive_rows)
        total_positive = len(df_positive_rows)
        unique_positive = df_positive_rows["registration"].nunique()

        status_counts = df_positive_rows["dbd_status"].value_counts()
        type_counts = df_positive_rows["transfer_type"].value_counts()

        st.caption("สรุปรายการก่อนกรอง")
        col_summary_left, col_summary_right = st.columns(2)
        with col_summary_left:
            st.metric("จำนวนแถว (ยอดบวก)", total_positive)
            st.metric("เลขทะเบียนไม่ซ้ำ", unique_positive)
        with col_summary_right:
            st.write("**สถานะข้อมูล DBD**")
            status_display_df = status_counts.rename_axis("สถานะ").reset_index(name="จำนวน")
            st.dataframe(status_display_df, height=150, hide_index=True)
        st.write("**ประเภทผู้ส่งโอน**")
        type_display_df = type_counts.rename_axis("ประเภท").reset_index(name="จำนวน")
        st.dataframe(type_display_df, height=150, hide_index=True)

        all_possible_statuses = ["มีข้อมูล", "ไม่มีข้อมูล", "ค่าว่าง", "ข้อผิดพลาด"]
        available_step2_statuses = sorted(
            {status for status in df_positive_rows["dbd_status"].dropna().unique().tolist() if status} |
            set(all_possible_statuses)
        )

        available_step2_types = sorted(
            {type_value for type_value in df_positive_rows["transfer_type"].dropna().unique().tolist() if type_value and type_value != "-"}
        )
        if df_positive_rows["transfer_type"].isin(["-"]).any() and "-" not in available_step2_types:
            available_step2_types.insert(0, "-")

        st.subheader("⚙️ ตัวกรองสำหรับขั้นตอนที่ 2")

        step1_status_pref = st.session_state.get("peakengine_status_filter", [])
        normalized_step1_pref = [
            status for status in step1_status_pref if status in available_step2_statuses
        ]
        snapshot_key = "peakengine_step1_status_filter_snapshot"
        step1_snapshot = tuple(sorted(normalized_step1_pref))
        previous_snapshot = st.session_state.get(snapshot_key)
        if step1_snapshot != previous_snapshot:
            st.session_state[snapshot_key] = step1_snapshot
            st.session_state["peakengine_step2_status_filter"] = normalized_step1_pref or available_step2_statuses
        elif "peakengine_step2_status_filter" not in st.session_state:
            st.session_state["peakengine_step2_status_filter"] = normalized_step1_pref or available_step2_statuses

        default_step2_status_filter = st.session_state.get("peakengine_step2_status_filter", available_step2_statuses)
        default_step2_status_filter = [status for status in default_step2_status_filter if status in available_step2_statuses]
        if not default_step2_status_filter:
            default_step2_status_filter = available_step2_statuses

        pending_status_filter = st.session_state.pop("peakengine_step2_status_filter_pending", None)
        if pending_status_filter is not None:
            st.session_state["peakengine_step2_status_filter"] = [status for status in pending_status_filter if status in available_step2_statuses]
            st.experimental_rerun()

        selected_step2_statuses = st.multiselect(
            "เลือกสถานะข้อมูล DBD ที่ต้องการให้ระบบกรอก:",
            options=available_step2_statuses,
            default=default_step2_status_filter,
            help="เลือกว่าอยากให้ระบบกรอกเฉพาะรายการที่มี/ไม่มีข้อมูล DBD หรือทั้งหมด",
            key="peakengine_step2_status_filter"
        )

        col_status_btn1, col_status_btn2 = st.columns(2)
        with col_status_btn1:
            if st.button("เลือกสถานะทั้งหมด", key="step2_status_select_all"):
                st.session_state["peakengine_step2_status_filter_pending"] = available_step2_statuses
        with col_status_btn2:
            if st.button("ล้างการเลือกสถานะ", key="step2_status_clear"):
                st.session_state["peakengine_step2_status_filter_pending"] = []

        default_step2_type_filter = st.session_state.get("peakengine_step2_type_filter", available_step2_types)
        default_step2_type_filter = [type_value for type_value in default_step2_type_filter if type_value in available_step2_types]
        if not default_step2_type_filter and available_step2_types:
            default_step2_type_filter = available_step2_types

        pending_type_filter = st.session_state.pop("peakengine_step2_type_filter_pending", None)
        if pending_type_filter is not None:
            st.session_state["peakengine_step2_type_filter"] = [value for value in pending_type_filter if value in available_step2_types]
            st.experimental_rerun()

        selected_step2_types = st.multiselect(
            "เลือกประเภทผู้ส่งโอนที่จะให้ระบบกรอก:",
            options=available_step2_types,
            default=default_step2_type_filter,
            help="กรองตามประเภทผู้ส่งโอนก่อนเริ่มบอท (ถ้าไม่เลือกจะใช้ทุกประเภท)",
            key="peakengine_step2_type_filter"
        )

        if available_step2_types:
            col_type_btn1, col_type_btn2 = st.columns(2)
            with col_type_btn1:
                if st.button("เลือกประเภททั้งหมด", key="step2_type_select_all"):
                    st.session_state["peakengine_step2_type_filter_pending"] = available_step2_types
            with col_type_btn2:
                if st.button("ล้างการเลือกประเภท", key="step2_type_clear"):
                    st.session_state["peakengine_step2_type_filter_pending"] = []

        step2_search_keyword = st.text_input(
            "ค้นหาเลขทะเบียนหรือชื่อบริษัท (ปล่อยว่างหากไม่ต้องการกรอง)",
            value=st.session_state.get("peakengine_step2_search", ""),
            key="peakengine_step2_search"
        ).strip()

        show_preview_step2 = st.checkbox(
            "แสดงรายการที่จะประมวลผล (หลังกรอง)",
            value=st.session_state.get("peakengine_step2_show_preview", False),
            key="peakengine_step2_show_preview"
        )

        filter_mask = pd.Series(True, index=df_positive_rows.index)
        if selected_step2_statuses:
            normalized_selected = [
                "ลูกค้าไม่ประสงค์ออกนาม" if status == "ไม่มีข้อมูล" else status
                for status in selected_step2_statuses
            ]
            filter_mask &= (
                df_positive_rows["dbd_status"].isin(selected_step2_statuses)
                & ~df_positive_rows["dbd_status"].astype(str).str.contains("ไม่มีข้อมูล", na=False)
            )

        if selected_step2_types and available_step2_types:
            filter_mask &= df_positive_rows["transfer_type"].isin(selected_step2_types)

        if step2_search_keyword:
            keyword_lower = step2_search_keyword.lower()
            filter_mask &= (
                df_positive_rows["registration"].str.contains(keyword_lower, case=False, na=False)
                | df_positive_rows["company_name"].str.contains(keyword_lower, case=False, na=False)
                | df_positive_rows["description"].str.contains(keyword_lower, case=False, na=False)
            )

        df_step2_filtered = df_positive_rows[filter_mask].copy().reset_index(drop=True)

        if df_step2_filtered.empty:
            st.warning("⚠️ ไม่มีเลขทะเบียนที่ตรงกับเงื่อนไขตัวกรองขั้นที่ 2")
            return

        if "row_key" in df_step2_filtered.columns:
            df_step2_filtered["row_key"] = (
                df_step2_filtered["row_key"]
                .astype(str)
                .fillna("")
                .apply(lambda x: x.strip())
            )
            missing_row_mask = df_step2_filtered["row_key"].isin(["", "nan", "none", "None"])
            if missing_row_mask.any():
                for idx_masked, _ in df_step2_filtered[missing_row_mask].iterrows():
                    df_step2_filtered.at[idx_masked, "row_key"] = f"row_{idx_masked}"
        else:
            df_step2_filtered["row_key"] = [f"row_{idx}" for idx in range(len(df_step2_filtered))]

        raw_not_found_mask = df_step2_filtered.get("dbd_info_raw", df_step2_filtered["dbd_status"]).astype(str).str.contains("ไม่พบข้อมูล", case=False, na=False)
        status_not_found_mask = df_step2_filtered["dbd_status"].astype(str).str.strip() == "ไม่มีข้อมูล"
        not_found_mask = raw_not_found_mask | status_not_found_mask

        company_display_series = df_step2_filtered["company_name"].astype(str).where(~not_found_mask, "ลูกค้าไม่ประสงค์ออกนาม")
        status_display_series = df_step2_filtered["dbd_status"].astype(str).where(~not_found_mask, "ลูกค้าไม่ประสงค์ออกนาม")

        df_step2_filtered["company_name_display"] = company_display_series
        df_step2_filtered["dbd_status_display"] = status_display_series

        filtered_total = len(df_step2_filtered)
        filtered_unique_total = df_step2_filtered["registration"].nunique()

        st.write(
            f"📌 พบเลขทะเบียนยอดบวกทั้งหมด {total_positive} รายการ (ไม่ซ้ำ {unique_positive}) "
            f"• หลังใช้ตัวกรองเหลือ {filtered_total} รายการ (ไม่ซ้ำ {filtered_unique_total})"
        )

        row_records = df_step2_filtered.to_dict("records")
        row_keys = [record.get("row_key") for record in row_records]
        registration_numbers = [record.get("registration") for record in row_records]
        unique_registration_numbers = list(dict.fromkeys(registration_numbers))
        row_key_map = {
            record.get("row_key"): record for record in row_records if record.get("row_key") is not None
        }

        not_found_df = df_step2_filtered[not_found_mask].copy()
        st.session_state["peakengine_step2_not_found_df"] = not_found_df
        st.session_state["peakengine_step2_not_found_regs"] = not_found_df["registration"].tolist()
        st.session_state["peakengine_step2_all_df"] = df_step2_filtered.copy()
        st.session_state["peakengine_step2_row_records"] = row_records
        st.session_state["peakengine_step2_row_key_map"] = row_key_map

        export_columns = {
            "registration": "เลขทะเบียน",
            "company_name": "ชื่อบริษัท/บุคคล (เดิม)",
            "company_name_display": "ชื่อบริษัท/บุคคล",
            "work_category": "ประเภทการทำงาน",
            "date": "วันที่",
            "amount": "จำนวนเงิน",
            "description": "คำอธิบาย",
            "dbd_status": "สถานะข้อมูล DBD (เดิม)",
            "dbd_status_display": "สถานะข้อมูล DBD",
            "transfer_type": "ประเภทผู้ส่งโอน",
            "dbd_company_name": "ชื่อบริษัทจาก DBD",
            "dbd_registration": "เลขทะเบียนจาก DBD",
            "dbd_business_type": "ประเภทธุรกิจ",
            "dbd_status_detail": "สถานะกิจการ",
            "dbd_capital": "ทุนจดทะเบียน",
            "dbd_address_text": "ที่อยู่ (ข้อความ)",
            "dbd_address_house_no": "ที่อยู่_บ้านเลขที่",
            "dbd_address_village": "ที่อยู่_หมู่บ้าน",
            "dbd_address_moo": "ที่อยู่_หมู่ที่",
            "dbd_address_subdistrict": "ที่อยู่_ตำบล",
            "dbd_address_district": "ที่อยู่_อำเภอ",
            "dbd_address_province": "ที่อยู่_จังหวัด",
            "dbd_address_postal_code": "ที่อยู่_รหัสไปรษณีย์",
            "dbd_info_raw": "ข้อมูล DBD (ดิบ)"
        }

        download_types = [
            value for value in df_step2_filtered["transfer_type"].dropna().unique().tolist()
            if str(value).strip()
        ]

        if download_types:
            st.subheader("💾 ดาวน์โหลดไฟล์แยกตามประเภทผู้ส่งโอน")
            st.caption("รวมเฉพาะรายการที่ผ่านตัวกรองขั้นตอนที่ 2 ในแต่ละไฟล์")

            export_datasets: List[Tuple[str, bytes, str]] = []
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                for type_value in sorted(download_types):
                    type_df = df_step2_filtered[df_step2_filtered["transfer_type"] == type_value]
                    if type_df.empty:
                        continue

                    export_df = type_df[list(export_columns.keys())].rename(columns=export_columns)

                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                        export_df.to_excel(writer, sheet_name="ข้อมูล", index=False)
                    excel_buffer.seek(0)

                    safe_type = slugify_filename(type_value)
                    excel_filename = f"step2_{safe_type}_{timestamp_str}.xlsx"

                    zip_file.writestr(excel_filename, excel_buffer.getvalue())
                    export_datasets.append((type_value, excel_buffer.getvalue(), excel_filename))

            if export_datasets:
                zip_buffer.seek(0)
                st.download_button(
                    label="📦 ดาวน์โหลดทุกประเภทเป็นไฟล์ ZIP",
                    data=zip_buffer.getvalue(),
                    file_name=f"step2_transfer_types_{timestamp_str}.zip",
                    mime="application/zip",
                    key="download_step2_zip_all"
                )

                st.caption("หรือดาวน์โหลดเฉพาะประเภทที่ต้องการ:")
                num_cols = min(3, len(export_datasets))
                cols = st.columns(num_cols)

                for idx, (type_value, excel_bytes, excel_filename) in enumerate(export_datasets):
                    target_col = cols[idx % num_cols]
                    with target_col:
                        st.download_button(
                            label=f"📥 ดาวน์โหลด {type_value}",
                            data=excel_bytes,
                            file_name=excel_filename,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_step2_{slugify_filename(f'{type_value}_{idx}')}"
                        )

        if show_preview_step2:
            preview_columns = {
                "registration": "เลขทะเบียน",
                "company_name_display": "ชื่อบริษัท/บุคคล",
                "work_category": "ประเภทการทำงาน",
                "date": "วันที่",
                "amount": "จำนวนเงิน",
                "description": "คำอธิบาย",
                "dbd_status_display": "สถานะข้อมูล DBD",
                "transfer_type": "ประเภทผู้ส่งโอน"
            }
            preview_df = df_step2_filtered[list(preview_columns.keys())].rename(columns=preview_columns)
            st.dataframe(
                preview_df.reset_index(drop=True),
                use_container_width=True,
                hide_index=True
            )

        st.session_state["peakengine_step2_filtered_df"] = df_step2_filtered

        existing_reg_info_map = st.session_state.get("peakengine_reg_info_map", {})
        reg_info_map_updated: Dict[str, Dict[str, Any]] = dict(existing_reg_info_map)
        row_payload_map: Dict[str, Dict[str, Any]] = {}

        if not df_source_for_filters.empty:
            for _, step2_row in df_step2_filtered.iterrows():
                reg_value = step2_row.get("registration")
                if not reg_value:
                    continue

                source_idx = step2_row.get("source_index")
                base_series = None
                if source_idx is not None and not (isinstance(source_idx, float) and pd.isna(source_idx)):
                    try:
                        base_series = df_source_for_filters.loc[source_idx]
                    except KeyError:
                        try:
                            base_series = df_source_for_filters.iloc[int(source_idx)]
                        except Exception:
                            base_series = None

                base_row_dict: Dict[str, Any] = {}
                if base_series is not None:
                    base_row_dict = base_series.to_dict()

                dbd_raw_candidates = [
                    base_row_dict.get("ข้อมูล DBD"),
                    step2_row.get("dbd_info_raw"),
                    existing_reg_info_map.get(reg_value, {}).get("dbd_raw")
                ]
                dbd_raw_text = pick_first_text(*dbd_raw_candidates) or ""
                dbd_parsed = parse_dbd_info(dbd_raw_text)

                combined_row: Dict[str, Any] = {}
                combined_row.update(base_row_dict)

                step2_row_dict = step2_row.to_dict()
                for key, value in step2_row_dict.items():
                    if key not in combined_row or not combined_row[key]:
                        combined_row[key] = value

                # Map step2-specific address columns back to original column names if missing
                address_mapping = {
                    "dbd_address_house_no": "ที่อยู่_บ้านเลขที่",
                    "dbd_address_village": "ที่อยู่_หมู่บ้าน",
                    "dbd_address_moo": "ที่อยู่_หมู่ที่",
                    "dbd_address_subdistrict": "ที่อยู่_ตำบล",
                    "dbd_address_district": "ที่อยู่_อำเภอ",
                    "dbd_address_province": "ที่อยู่_จังหวัด",
                    "dbd_address_postal_code": "ที่อยู่_รหัสไปรษณีย์"
                }
                for source_key, target_key in address_mapping.items():
                    value = step2_row_dict.get(source_key)
                    if value and (target_key not in combined_row or not combined_row[target_key]):
                        combined_row[target_key] = value

                if dbd_raw_text and ("ข้อมูล DBD" not in combined_row or not combined_row["ข้อมูล DBD"]):
                    combined_row["ข้อมูล DBD"] = dbd_raw_text

                if step2_row_dict.get("company_name_display"):
                    combined_row.setdefault("ชื่อบริษัท/บุคคล", step2_row_dict.get("company_name_display"))
                if step2_row_dict.get("work_category"):
                    combined_row.setdefault("ประเภทการทำงาน", step2_row_dict.get("work_category"))
                if step2_row_dict.get("document_date_raw") is not None:
                    if "วันที่" not in combined_row or not combined_row.get("วันที่"):
                        combined_row["วันที่"] = step2_row_dict.get("document_date_raw")
                if step2_row_dict.get("amount_numeric") is not None and (
                    "amount_numeric" not in combined_row or pd.isna(combined_row.get("amount_numeric"))
                ):
                    combined_row["amount_numeric"] = step2_row_dict.get("amount_numeric")
                if step2_row_dict.get("amount") and (
                    "amount" not in combined_row or not combined_row.get("amount")
                ):
                    combined_row["amount"] = step2_row_dict.get("amount")

                if step2_row_dict.get("dbd_company_name") and (
                    "ชื่อบริษัทจาก DBD" not in combined_row or not combined_row["ชื่อบริษัทจาก DBD"]
                ):
                    combined_row["ชื่อบริษัทจาก DBD"] = step2_row_dict.get("dbd_company_name")

                if step2_row_dict.get("dbd_registration") and (
                    "เลขทะเบียนจาก DBD" not in combined_row or not combined_row["เลขทะเบียนจาก DBD"]
                ):
                    combined_row["เลขทะเบียนจาก DBD"] = step2_row_dict.get("dbd_registration")

                reg_entry = reg_info_map_updated.get(reg_value, {})
                reg_entry.update({
                    "registration": reg_value,
                    "transfer_type": step2_row_dict.get("transfer_type") or reg_entry.get("transfer_type", ""),
                    "dbd_status": step2_row_dict.get("dbd_status") or reg_entry.get("dbd_status", ""),
                    "dbd_status_display": step2_row_dict.get("dbd_status_display") or reg_entry.get("dbd_status_display", step2_row_dict.get("dbd_status", "")),
                    "company_name": step2_row_dict.get("company_name_display") or step2_row_dict.get("company_name") or reg_entry.get("company_name", ""),
                    "company_name_display": step2_row_dict.get("company_name_display") or reg_entry.get("company_name_display", step2_row_dict.get("company_name")),
                    "work_category": step2_row_dict.get("work_category") or reg_entry.get("work_category", ""),
                    "document_date_raw": step2_row_dict.get("document_date_raw") if step2_row_dict.get("document_date_raw") is not None else reg_entry.get("document_date_raw"),
                    "amount": step2_row_dict.get("amount") or reg_entry.get("amount", ""),
                    "amount_numeric": step2_row_dict.get("amount_numeric") if step2_row_dict.get("amount_numeric") is not None else reg_entry.get("amount_numeric"),
                    "dbd_raw": dbd_raw_text or reg_entry.get("dbd_raw", ""),
                    "dbd_info": dbd_parsed or reg_entry.get("dbd_info", {}),
                    "row_index": (
                        int(source_idx) if source_idx is not None and not (isinstance(source_idx, float) and pd.isna(source_idx))
                        else reg_entry.get("row_index")
                    ),
                    "row_key": step2_row_dict.get("row_key") or reg_entry.get("row_key"),
                    "row": combined_row,
                    "step2_row": step2_row_dict
                })
                reg_info_map_updated[reg_value] = reg_entry

                row_key_value = step2_row_dict.get("row_key")
                if row_key_value:
                    row_payload_map[row_key_value] = {
                        "registration": reg_value,
                        "transfer_type": step2_row_dict.get("transfer_type") or reg_entry.get("transfer_type", ""),
                        "dbd_status": step2_row_dict.get("dbd_status"),
                        "dbd_status_display": step2_row_dict.get("dbd_status_display"),
                        "company_name": reg_entry.get("company_name"),
                        "company_name_display": reg_entry.get("company_name_display"),
                        "work_category": step2_row_dict.get("work_category"),
                        "document_date_raw": step2_row_dict.get("document_date_raw"),
                        "amount": step2_row_dict.get("amount"),
                        "amount_numeric": step2_row_dict.get("amount_numeric"),
                        "dbd_raw": dbd_raw_text,
                        "dbd_info": dbd_parsed,
                        "row_index": (
                            int(source_idx) if source_idx is not None and not (isinstance(source_idx, float) and pd.isna(source_idx))
                            else reg_entry.get("row_index")
                        ),
                        "row": combined_row,
                        "step2_row": step2_row_dict
                    }

        st.session_state["peakengine_reg_info_map"] = reg_info_map_updated
        st.session_state["peakengine_row_payload_map"] = row_payload_map

        if not registration_numbers:
            st.warning("⚠️ ไม่มีเลขทะเบียนที่พร้อมสำหรับกรอก")
            return

        run_summary_stats = {
            "total_rows": filtered_total,
            "unique_regs": filtered_unique_total,
            "not_found_rows": 0,
            "not_found_unique": 0
        }
        st.session_state["peakengine_run_summary_stats"] = run_summary_stats
        st.session_state["peakengine_run_summary_snapshot"] = df_step2_filtered

        summary_tab, runner_tab = st.tabs(["📊 สรุปข้อมูลสำหรับการรันบอท", "⚙️ ตั้งค่าการรันบอท"])

        with summary_tab:
            col_summary_metrics = st.columns(3)
            with col_summary_metrics[0]:
                st.metric("รายการหลังกรอง", filtered_total)
            with col_summary_metrics[1]:
                st.metric("เลขทะเบียนไม่ซ้ำ", filtered_unique_total)

            status_distribution = df_step2_filtered["dbd_status_display"].value_counts().rename_axis("สถานะ").reset_index(name="จำนวน")
            type_distribution = df_step2_filtered["transfer_type"].value_counts().rename_axis("ประเภทผู้ส่งโอน").reset_index(name="จำนวน")

            st.write("**สัดส่วนสถานะข้อมูล DBD (หลังกรอง)**")
            st.dataframe(status_distribution, use_container_width=True, hide_index=True)

            st.write("**สัดส่วนประเภทผู้ส่งโอน (หลังกรอง)**")
            st.dataframe(type_distribution, use_container_width=True, hide_index=True)

            with st.expander("ดูรายการที่ DBD ไม่พบข้อมูล", expanded=False):
                if not not_found_df.empty:
                    st.dataframe(
                        not_found_df.reset_index(drop=True)[
                            ["registration", "company_name_display", "work_category", "date", "amount", "description", "transfer_type", "dbd_info_raw"]
                        ].rename(columns={
                            "registration": "เลขทะเบียน",
                            "company_name_display": "ชื่อบริษัท/บุคคล",
                            "work_category": "ประเภทการทำงาน",
                            "date": "วันที่",
                            "amount": "จำนวนเงิน",
                            "description": "คำอธิบาย",
                            "transfer_type": "ประเภทผู้ส่งโอน",
                            "dbd_info_raw": "ข้อมูล DBD"
                        }),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("ไม่มีรายการที่ระบุว่า DBD ไม่พบข้อมูลในตัวกรองปัจจุบัน")

            st.write("**ตัวอย่างรายการสำหรับบอท (20 แถวแรก)**")
            st.dataframe(
                df_step2_filtered.head(20)[
                    ["registration", "company_name_display", "work_category", "date", "amount", "description", "dbd_status_display", "transfer_type", "dbd_info_raw"]
                ].rename(columns={
                    "registration": "เลขทะเบียน",
                    "company_name_display": "ชื่อบริษัท/บุคคล",
                    "work_category": "ประเภทการทำงาน",
                    "date": "วันที่",
                    "amount": "จำนวนเงิน",
                    "description": "คำอธิบาย",
                    "dbd_status_display": "สถานะข้อมูล DBD",
                    "transfer_type": "ประเภทผู้ส่งโอน",
                    "dbd_info_raw": "ข้อมูล DBD"
                }),
                use_container_width=True,
                hide_index=True
            )

        with runner_tab:
            processed_row_keys_list = st.session_state.get("peakengine_processed_row_keys", [])
            if not isinstance(processed_row_keys_list, list):
                processed_row_keys_list = []
            processed_row_keys_set = set(processed_row_keys_list)

            pending_row_records = [
                record for record in row_records
                if record.get("row_key") not in processed_row_keys_set
            ]
            pending_numbers = [
                record.get("registration") for record in pending_row_records
                if record.get("registration")
            ]
            pending_unique_numbers = list(dict.fromkeys(pending_numbers))

            col_stats1, col_stats2, col_stats3 = st.columns(3)
            with col_stats1:
                st.metric("รายการที่กรอกแล้ว", len(processed_row_keys_set))
            with col_stats2:
                st.metric("รายการที่เหลือ", len(pending_row_records))
            with col_stats3:
                st.metric("เลขทะเบียนที่เหลือ (ไม่ซ้ำ)", len(pending_unique_numbers))

            st.caption("เลือกโหมดการทำงาน: กรอกทีละรายการ หรือกรอกต่อเนื่องทุกเลขที่เหลือ")

            fill_mode = st.radio(
                "โหมดการกรอก:",
                ["กรอกทีละรายการ", "กรอกต่อเนื่อง (ทั้งหมดที่เหลือ)"],
                key="peak_fill_mode",
                horizontal=True
            )

            run_summary_stats = st.session_state.get("peakengine_run_summary_stats", {})
            run_summary_df = st.session_state.get("peakengine_run_summary_snapshot")

            st.subheader("📋 รายการที่จะให้บอทรัน")
            st.write(
                f"- รายการทั้งหมดที่ผ่านตัวกรอง: {run_summary_stats.get('total_rows', filtered_total)} "
                f"(เลขทะเบียนไม่ซ้ำ {run_summary_stats.get('unique_regs', filtered_unique_total)})"
            )
            st.write(
                "- รายการที่ DBD ระบุว่าไม่พบข้อมูล: 0 "
                "(เลขทะเบียนไม่ซ้ำ 0)"
            )
            processed_unique_regs = set()
            for row_key in processed_row_keys_set:
                record = row_key_map.get(row_key)
                if record and record.get("registration"):
                    processed_unique_regs.add(record.get("registration"))
            st.write(f"- รายการที่เคยรันไปแล้ว: {len(processed_row_keys_set)} (เลขทะเบียนไม่ซ้ำ {len(processed_unique_regs)})")
            st.write(f"- รายการที่เหลือสำหรับบอทในรอบนี้: {len(pending_row_records)} (เลขทะเบียนไม่ซ้ำ {len(pending_unique_numbers)})")
            pending_not_found = [
                reg for reg in pending_unique_numbers
                if run_summary_df is not None
                and not run_summary_df.empty
                and any(
                    (run_summary_df["registration"] == reg) &
                    (
                        run_summary_df["dbd_status_display"].astype(str).str.strip() == "ลูกค้าไม่ประสงค์ออกนาม"
                    )
                )
            ]
            if pending_not_found:
                st.write(f"- เลขทะเบียนที่ต้องเจอสถานะ DBD 'ไม่พบข้อมูล': {len(pending_not_found)}")
                st.code(", ".join(pending_not_found[:20]), language=None)
                if len(pending_not_found) > 20:
                    st.caption(f"... และอีก {len(pending_not_found) - 20} รายการ")

            selected_registration = None
            selected_record = None
            if fill_mode == "กรอกทีละรายการ":
                def format_row_option(row_key: str) -> str:
                    record = row_key_map.get(row_key, {})
                    registration_value = record.get("registration", "-")
                    company_value = record.get("company_name_display", "-")
                    date_value = record.get("date", "-")
                    amount_value = record.get("amount", "-")
                    work_category_value = record.get("work_category", "-")
                    return f"{registration_value} • {company_value} • {date_value} • {amount_value} • {work_category_value}"

                available_row_keys = [
                    record.get("row_key") for record in pending_row_records
                    if record.get("row_key") is not None
                ]
                if not available_row_keys and row_records:
                    available_row_keys = [
                        record.get("row_key") for record in row_records
                        if record.get("row_key") is not None
                    ]

                if not available_row_keys:
                    st.warning("⚠️ ไม่มีรายการให้เลือกสำหรับการกรอก")
                    return

                selected_row_key = st.selectbox(
                    "เลือกเลขทะเบียนที่จะกรอก:",
                    available_row_keys,
                    index=0 if available_row_keys else None,
                    key="peakengine_selected_row_key",
                    format_func=format_row_option
                )

                if not selected_row_key:
                    st.warning("⚠️ ไม่พบเลขทะเบียนที่พร้อมสำหรับกรอก")
                    return
                selected_record = row_key_map.get(selected_row_key)
                if not selected_record:
                    st.warning("⚠️ ไม่พบข้อมูลของรายการที่เลือก")
                    return
                selected_registration = selected_record.get("registration")
                target_row_records = [selected_record]
            else:
                st.session_state["peakengine_selected_row_key"] = ""
                target_row_records = pending_row_records

            if st.button("♻️ รีเซ็ตสถานะรายการที่กรอกแล้ว", key="reset_peak_processed"):
                st.session_state["peakengine_processed_regs"] = []
                st.session_state["peakengine_processed_row_keys"] = []
                st.success("รีเซ็ตสถานะเรียบร้อยแล้ว")
                st.experimental_rerun()

            log_expander = st.expander("📋 Log การทำงาน", expanded=False)
            log_placeholder = log_expander.empty()
            log_messages: List[Dict[str, str]] = []

            def peak_log(message: str, status: str = "info"):
                icon_map = {
                    "info": "ℹ️",
                    "success": "✅",
                    "warning": "⚠️",
                    "error": "❌"
                }
                log_messages.append({
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "status": status,
                    "message": message
                })
                lines = []
                for entry in log_messages[-200:]:
                    icon = icon_map.get(entry["status"], "📝")
                    lines.append(f"[{entry['time']}] {icon} {entry['message']}")
                log_placeholder.code("\n".join(lines), language=None)

            col_fill_peak, col_newpeak = st.columns(2)
            with col_fill_peak:
                if st.button("📝 เริ่มกรอกเลขทะเบียนลงหน้าเว็บ PEAK", type="primary", key="fill_peak_contacts_btn"):
                    if config is None:
                        st.error("❌ ไม่พบไฟล์ config.py ไม่สามารถเข้าสู่ระบบ PEAKEngine ได้")
                        return

                    username = getattr(config, 'PEAKENGINE_USERNAME', '')
                    password = getattr(config, 'PEAKENGINE_PASSWORD', '')
                    link_company = getattr(config, 'Link_conpany', None)
                    link_receipt = getattr(config, 'Link_receipt', None)
                    headless = getattr(config, 'HEADLESS_MODE', False)

                    if not username or not password:
                        st.error("❌ กรุณากำหนด PEAKENGINE_USERNAME และ PEAKENGINE_PASSWORD ใน config.py ก่อน")
                        return

                    with st.spinner("กำลังเปิดเบราว์เซอร์และเข้าสู่ระบบ..."):
                        try:
                            from peakengine_bot import PeakEngineBot
                            bot = PeakEngineBot(use_browser=True, headless=headless)
                            _peakengine_bots.append(bot)

                            peak_log("🚀 เริ่มเข้าสู่ระบบ PEAKEngine...", "info")
                            login_success = bot.login(
                                username,
                                password,
                                link_company=link_company,
                                link_receipt=link_receipt,
                                log_callback=peak_log
                            )

                            if not login_success:
                                st.error("❌ เข้าสู่ระบบ PEAKEngine ไม่สำเร็จ กรุณาตรวจสอบข้อมูลใน config.py")
                                return

                            peak_log("✅ เข้าสู่ระบบเรียบร้อย เตรียมกรอกเลขทะเบียน", "success")

                            if fill_mode == "กรอกทีละรายการ":
                                fill_targets = [selected_registration] if selected_registration else []
                                target_row_keys = []
                                if selected_record and selected_record.get("row_key") is not None:
                                    target_row_keys.append(selected_record.get("row_key"))
                            else:
                                if not target_row_records:
                                    st.warning("⚠️ ไม่มีรายการที่เหลือสำหรับกรอก")
                                    return
                                fill_targets = []
                                target_row_keys = []
                                for record in target_row_records:
                                    reg_candidate = record.get("registration")
                                    row_key_candidate = record.get("row_key")
                                    if not reg_candidate or row_key_candidate is None:
                                        continue
                                    fill_targets.append(reg_candidate)
                                    target_row_keys.append(row_key_candidate)
                                if not fill_targets:
                                    st.warning("⚠️ ไม่มีเลขทะเบียนที่สามารถกรอกได้")
                                    return
                                st.info(f"🔁 จะกรอกเลขทะเบียนทั้งหมด {len(fill_targets)} รายการที่ยังไม่ได้ทำ")

                            reg_info_map = st.session_state.get("peakengine_reg_info_map", {})
                            row_payload_map_session = st.session_state.get("peakengine_row_payload_map", {})
                            fill_result = bot.fill_contact_fields(
                                fill_targets,
                                reg_info_map=reg_info_map,
                                row_keys=target_row_keys,
                                row_payload_map=row_payload_map_session,
                                log_callback=peak_log
                            )

                            if "error" in fill_result:
                                st.error(f"❌ ไม่สามารถกรอกข้อมูลได้: {fill_result['error']}")
                            else:
                                success_count = fill_result.get("success", 0)
                                total_count = fill_result.get("total", len(fill_targets))
                                error_list = fill_result.get("errors", [])
                                processed_values = fill_result.get("processed", [])

                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("จำนวนที่ต้องกรอก", total_count)
                                with col2:
                                    st.metric("กรอกสำเร็จ", success_count)
                                with col3:
                                    st.metric("ไม่สำเร็จ", len(error_list))

                                processed_row_keys_list = fill_result.get("processed_row_keys", [])
                                if processed_row_keys_list:
                                    processed_row_keys_set.update(processed_row_keys_list)
                                    st.session_state["peakengine_processed_row_keys"] = list(processed_row_keys_set)
                                if processed_values:
                                    processed_regs_list = st.session_state.get("peakengine_processed_regs", [])
                                    processed_regs_list.extend(processed_values)
                                    st.session_state["peakengine_processed_regs"] = processed_regs_list
                                receipt_links = fill_result.get("receipt_links", [])
                                if receipt_links:
                                    stored_links = st.session_state.get("peakengine_receipt_links", [])
                                    if not isinstance(stored_links, list):
                                        stored_links = []
                                    stored_links.extend(receipt_links)
                                    st.session_state["peakengine_receipt_links"] = stored_links

                                if error_list:
                                    st.warning("⚠️ มีบางรายการกรอกไม่สำเร็จ ดูรายละเอียดด้านล่าง")
                                    df_errors = pd.DataFrame(error_list)
                                    st.dataframe(df_errors, use_container_width=True)
                                else:
                                    if fill_mode == "กรอกทีละรายการ":
                                        st.success("🎉 กรอกเลขทะเบียนเสร็จสิ้น")
                                    else:
                                        st.success("🎉 กรอกเลขทะเบียนที่เหลือทั้งหมดเรียบร้อย")

                                dropdown_data = fill_result.get("dropdown_options", [])
                                if dropdown_data:
                                    st.subheader("📋 ผลการตรวจสอบ Dropdown หลังกรอก")
                                    for entry in dropdown_data:
                                        value = entry.get("value", "")
                                        items = entry.get("items", [])
                                        st.write(f"**เลขทะเบียน:** {value}")
                                        if items:
                                            st.write(f"ตัวเลือก ({len(items)}):")
                                            st.code("\n".join(items), language=None)
                                        else:
                                            st.write("ไม่มีตัวเลือกแสดงใน dropdown")

                                plus_clicks = fill_result.get("plus_clicked", [])
                                if plus_clicks:
                                    st.info(f"🔄 มีการคลิก '+ เพิ่มผู้ติดต่อ' สำหรับเลขทะเบียน: {', '.join(plus_clicks)}")
                                elif dropdown_data:
                                    st.info("ℹ️ ไม่มีการคลิก '+ เพิ่มผู้ติดต่อ' เนื่องจากพบตัวเลือกอื่นใน dropdown")

                                selected_existing = fill_result.get("selected_existing", [])
                                if selected_existing:
                                    st.success(f"✅ ระบบเลือกผู้ติดต่อที่มีอยู่แล้วสำหรับเลขทะเบียน: {', '.join(selected_existing)}")

                            validation_results = fill_result.get("validation", [])
                            if validation_results:
                                st.subheader("🔍 ผลการตรวจสอบข้อมูลกับ Excel")
                                for validation in validation_results:
                                    reg = validation.get("registration", "")
                                    st.write(f"**เลขทะเบียน:** {reg}")
                                    overall = validation.get("overall_match", False)
                                    if overall:
                                        st.success("ข้อมูลที่ค้นหามาตรงกับข้อมูลใน Excel ทุกช่องที่ตรวจสอบ")
                                    else:
                                        st.warning("พบข้อมูลบางช่องไม่ตรงกับ Excel ดูรายละเอียดด้านล่าง")
                                    details = validation.get("details", [])
                                    mismatch_rows = []
                                    for detail in details:
                                        field = detail.get("field", "")
                                        expected = detail.get("expected", "")
                                        actual = detail.get("actual", "")
                                        match = detail.get("match", False)
                                        status_symbol = "✅" if match else "⚠️"
                                        mismatch_rows.append(f"{status_symbol} {field}\n  - Excel: {expected}\n  - ระบบ: {actual}")
                                    st.code("\n".join(mismatch_rows), language=None)

                        except Exception as e:
                            st.error(f"❌ เกิดข้อผิดพลาดระหว่างกรอกข้อมูล: {str(e)}")
                            peak_log(f"❌ เกิดข้อผิดพลาด: {str(e)}", "error")

        stored_links = st.session_state.get("peakengine_receipt_links", [])
        if stored_links:
            st.subheader("📄 ลิงก์ใบเสร็จที่บันทึกไว้")
            df_receipts = pd.DataFrame(stored_links)
            st.dataframe(df_receipts, use_container_width=True)
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                df_receipts.to_excel(writer, index=False, sheet_name="Receipts")
            excel_buffer.seek(0)
            st.download_button(
                label="💾 ดาวน์โหลดลิงก์ใบเสร็จ (Excel)",
                data=excel_buffer.getvalue(),
                file_name="receipt_links.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_receipt_links_excel"
            )

        with col_newpeak:
            if st.button("🆕 เปิดระบบ New Peak (ทดลอง)", key="open_newpeak_from_excel"):
                with st.spinner("กำลังเปิดเบราว์เซอร์ New Peak..."):
                    if open_newpeak_login():
                        st.success("✅ สั่งเปิด Browser สำหรับระบบ New Peak เรียบร้อยแล้ว")
                        st.info("👀 ตรวจสอบหน้าต่างเบราว์เซอร์ใหม่เพื่อดูการทำงานของบอท New Peak")
                        with st.spinner("⏳ กำลังตรวจสอบอินสแตนซ์ NewPeakBot..."):
                            newpeak_bot_instance = wait_for_newpeak_instance(timeout=45, poll_interval=0.5)
                            if newpeak_bot_instance and isinstance(newpeak_bot_instance, NewPeakBot):
                                st.session_state["active_newpeak_bot"] = newpeak_bot_instance
                                peak_log("✅ พบอินสแตนซ์ NewPeakBot พร้อมใช้งาน", "success")
                            else:
                                st.warning("⚠️ ไม่พบอินสแตนซ์ NewPeakBot ที่เพิ่งเปิด กรุณาลองกดปุ่มอีกครั้งหรือตรวจสอบ log")
                                peak_log("⚠️ ไม่พบอินสแตนซ์ NewPeakBot ที่พร้อมใช้งานหลังสั่งเปิด", "warning")
                                return
                        df_source = st.session_state.get("peakengine_source_df")
                        if df_source is None or df_source.empty:
                            st.warning("⚠️ ไม่มีข้อมูล Excel ที่โหลดไว้สำหรับประมวลผล")
                        else:
                            amount_col = None
                            if "ยอดเงิน_numeric" in df_source.columns:
                                amount_col = "ยอดเงิน_numeric"
                            elif "จำนวนเงิน" in df_source.columns:
                                amount_col = "จำนวนเงิน"
                            type_col = "ประเภทผู้ส่งโอน" if "ประเภทผู้ส่งโอน" in df_source.columns else None
                            dbd_col = "ข้อมูล DBD" if "ข้อมูล DBD" in df_source.columns else None
                            company_col = None
                            for candidate in ["ชื่อบริษัทจาก DBD", "ชื่อบัญชีจาก DBD", "ชื่อบริษัท/บุคคล"]:
                                if candidate in df_source.columns:
                                    company_col = candidate
                                    break

                            if not amount_col or not type_col or not dbd_col:
                                missing_cols = []
                                if not amount_col:
                                    missing_cols.append("จำนวนเงิน หรือ ยอดเงิน_numeric")
                                if not type_col:
                                    missing_cols.append("ประเภทผู้ส่งโอน")
                                if not dbd_col:
                                    missing_cols.append("ข้อมูล DBD")
                                st.warning(f"⚠️ ไม่พบคอลัมน์ที่จำเป็น: {', '.join(missing_cols)}")
                            else:
                                try:
                                    newpeak_bot = st.session_state.get("active_newpeak_bot")
                                    if not isinstance(newpeak_bot, NewPeakBot):
                                        newpeak_bot = wait_for_newpeak_instance(timeout=30, poll_interval=0.5)
                                        if isinstance(newpeak_bot, NewPeakBot):
                                            st.session_state["active_newpeak_bot"] = newpeak_bot
                                            peak_log("ℹ️ ใช้ NewPeakBot ล่าสุดจากคิว", "info")
                                    if not isinstance(newpeak_bot, NewPeakBot):
                                        st.warning("⚠️ ไม่พบอินสแตนซ์ NewPeakBot ที่พร้อมใช้งาน")
                                        peak_log("⚠️ ไม่พบอินสแตนซ์ NewPeakBot ที่พร้อมใช้งาน", "warning")
                                    else:
                                        with st.spinner("⏳ กำลังรอให้ NewPeakBot เข้าสู่ระบบ..."):
                                            if not wait_for_newpeak_login(newpeak_bot, timeout=90, poll_interval=0.5, log_callback=peak_log):
                                                st.warning("⚠️ ระบบยังไม่ได้เข้าสู่ระบบ New Peak ภายในเวลาที่กำหนด")
                                                peak_log("⚠️ ระบบยังไม่ได้เข้าสู่ระบบ New Peak ภายในเวลาที่กำหนด", "warning")
                                                return
                                        peak_log("✅ พร้อมเริ่มประมวลผลข้อมูลด้วย NewPeakBot", "success")
                                        tasks, skipped_records = newpeak_bot.prepare_transaction_tasks(
                                            df_source.copy(),
                                            amount_column=amount_col,
                                            type_column=type_col,
                                            dbd_column=dbd_col,
                                            company_column=company_col,
                                        )

                                        selected_registration = st.session_state.get("selected_registration_number", "")
                                        fill_mode_value = st.session_state.get("peak_fill_mode")

                                        def normalize_registration(value: Any) -> str:
                                            if value is None or (isinstance(value, float) and pd.isna(value)):
                                                return ""
                                            text = str(value).strip()
                                            digits = "".join(ch for ch in text if ch.isdigit())
                                            if len(digits) >= 13:
                                                return digits[-13:]
                                            if len(digits) == 0:
                                                return ""
                                            return digits.zfill(13)

                                        def normalize_company_name(value: Any) -> str:
                                            if value is None or (isinstance(value, float) and pd.isna(value)):
                                                return ""
                                            text = str(value).lower()
                                            replacements = [
                                                "บริษัท",
                                                "จำกัด",
                                                "มหาชน",
                                                "(มหาชน)",
                                                "ห้างหุ้นส่วน",
                                                "หจก.",
                                                "บจก.",
                                                "คอร์ปอเรชั่น",
                                            ]
                                            for token in replacements:
                                                text = text.replace(token.lower(), " ")
                                            text = re.sub(r"[\"'.,()]", " ", text)
                                            text = re.sub(r"\s+", " ", text)
                                            return text.strip()

                                        registrations_in_tasks = [
                                            normalize_registration(task.get("registration"))
                                            for task in tasks
                                            if task.get("registration")
                                        ]
                                        if registrations_in_tasks:
                                            preview_sample = ", ".join(registrations_in_tasks[:10])
                                            if len(registrations_in_tasks) > 10:
                                                preview_sample += ", ..."
                                            peak_log(
                                                f"🗂 พบเลขทะเบียนในรายการพร้อมใช้งาน: {preview_sample}",
                                                "info",
                                            )
                                        else:
                                            peak_log(
                                                "⚠️ ไม่มีเลขทะเบียนในรายการที่ผ่านเงื่อนไข (ตรวจสอบคอลัมน์ข้อมูล DBD และจำนวนเงิน/ประเภทผู้ส่งโอน)",
                                                "warning",
                                            )

                                        if fill_mode_value == "กรอกทีละรายการ":
                                            normalized_selected = normalize_registration(selected_registration)
                                            if normalized_selected:
                                                filtered_tasks = [
                                                    task for task in tasks
                                                    if normalize_registration(task.get("registration")) == normalized_selected
                                                ]
                                                if not filtered_tasks:
                                                    # หาแถวใน Excel ที่มีเลขทะเบียนตรงกันเพื่อแสดงข้อมูลประกอบ
                                                    df_matches = pd.DataFrame()
                                                    selected_rows = pd.DataFrame()
                                                    if dbd_col in df_source.columns:
                                                        selected_rows = df_source[
                                                            df_source[dbd_col]
                                                            .astype(str)
                                                            .str.replace(r"\D", "", regex=True)
                                                            .str[-13:]
                                                            .fillna("")
                                                            == normalized_selected
                                                        ]
                                                        df_matches = selected_rows.copy()
                                                    # ถ้าไม่เจอด้วยเลขทะเบียน ให้ลองเทียบตามชื่อบริษัทจาก DBD
                                                    if df_matches.empty and company_col:
                                                        normalized_company_series = (
                                                            df_source[company_col]
                                                            .astype(str)
                                                            .apply(normalize_company_name)
                                                        )
                                                        target_names: List[str] = []
                                                        if not selected_rows.empty and company_col in selected_rows.columns:
                                                            target_names = (
                                                                selected_rows[company_col]
                                                                .astype(str)
                                                                .apply(normalize_company_name)
                                                                .unique()
                                                                .tolist()
                                                            )
                                                        if target_names:
                                                            df_matches = df_source[normalized_company_series.isin(target_names)]
                                                    if not df_matches.empty:
                                                        peak_log(
                                                            "ℹ️ พบแถวใน Excel ที่เลขทะเบียนตรงกัน "
                                                            "แต่ไม่ผ่านเงื่อนไข (อาจเป็นยอดเงินหรือลักษณะผู้ส่งโอน)",
                                                            "info",
                                                        )
                                                        st.info(
                                                            "ℹ️ พบแถวใน Excel ที่เลขทะเบียนตรงกัน "
                                                            "แต่ไม่ผ่านเงื่อนไข (ตรวจสอบยอดเงิน/ประเภทผู้ส่งโอน/ข้อมูล DBD)"
                                                        )
                                                        display_cols = [
                                                            col
                                                            for col in df_matches.columns
                                                            if col
                                                            in [
                                                                "วันที่",
                                                                amount_col,
                                                                type_col,
                                                                dbd_col,
                                                            ]
                                                        ]
                                                        if company_col and company_col in df_matches.columns:
                                                            display_cols.insert(0, company_col)
                                                        st.dataframe(
                                                            df_matches[display_cols],
                                                            use_container_width=True,
                                                        )
                                                    peak_log(
                                                        f"⚠️ ไม่พบรายการใน Excel ที่ตรงกับเลขทะเบียน {normalized_selected}",
                                                        "warning",
                                                    )
                                                    st.warning(
                                                        f"⚠️ ไม่พบรายการใน Excel ที่ตรงกับเลขทะเบียน {normalized_selected}"
                                                    )
                                                    return
                                                else:
                                                    # กรณีพบ task ให้เชื่อมโยงกับชีตสรุปข้อมูล DBD เพื่อดึงเลขทะเบียน 13 หลัก
                                                    summary_df = st.session_state.get("peakengine_summary_df")
                                                    summary_registration = ""
                                                    matched_row_indices = [task.get("row_index") for task in filtered_tasks if task.get("row_index") is not None]
                                                    matched_rows = (
                                                        df_source.loc[matched_row_indices]
                                                        if matched_row_indices
                                                        else pd.DataFrame()
                                                    )
                                                    company_name_candidates = []
                                                    task_company_candidates = [
                                                        normalize_company_name(task.get("company_name"))
                                                        for task in filtered_tasks
                                                        if task.get("company_name")
                                                    ]
                                                    company_name_candidates.extend(
                                                        [name for name in task_company_candidates if name]
                                                    )
                                                    if not matched_rows.empty and company_col and company_col in matched_rows.columns:
                                                        company_name_candidates = (
                                                            matched_rows[company_col]
                                                            .astype(str)
                                                            .apply(normalize_company_name)
                                                            .tolist()
                                                        )
                                                    if isinstance(summary_df, pd.DataFrame) and not summary_df.empty:
                                                        name_column = None
                                                        for candidate_col in ["ชื่อบริษัทจาก DBD", "ชื่อบริษัท", "ชื่อนิติบุคคล"]:
                                                            if candidate_col in summary_df.columns:
                                                                name_column = candidate_col
                                                                break
                                                        normalized_names = (
                                                            summary_df[name_column].astype(str).apply(normalize_company_name)
                                                            if name_column
                                                            else pd.Series(dtype=str)
                                                        )
                                                        summary_df["_normalized_name"] = normalized_names
                                                        summary_df["_reg_digits"] = (
                                                            summary_df.get("เลขทะเบียน_พร้อมใช้งาน", summary_df.get("เลขทะเบียน"))
                                                            .astype(str)
                                                            .str.replace(r"\D", "", regex=True)
                                                            .str[-13:]
                                                            .fillna("")
                                                        )
                                                        candidates = summary_df[
                                                            summary_df["_reg_digits"] == normalized_selected
                                                        ]
                                                        if candidates.empty and company_name_candidates:
                                                            candidates = summary_df[
                                                                summary_df["_normalized_name"].isin(company_name_candidates)
                                                            ]
                                                            if not candidates.empty:
                                                                summary_registration = candidates["_reg_digits"].iloc[0]
                                                                st.success(
                                                                    f"✅ พบเลขทะเบียนในชีต 'สรุปข้อมูล DBD': {summary_registration}"
                                                                )
                                                                st.dataframe(
                                                                    candidates[
                                                                        [
                                                                            col
                                                                            for col in candidates.columns
                                                                            if col
                                                                            in [
                                                                                "ชื่อบริษัทจาก DBD",
                                                                                "เลขทะเบียน",
                                                                                "เลขทะเบียน_พร้อมใช้งาน",
                                                                                "ประเภทธุรกิจ",
                                                                                "สถานะ",
                                                                                "ทุนจดทะเบียน",
                                                                            ]
                                                                        ]
                                                                    ],
                                                                    use_container_width=True,
                                                                )
                                                    # แสดงรายละเอียดจากข้อมูล DBD ที่ parse แล้ว
                                                    details_list = [
                                                        task.get("dbd_details", {})
                                                        for task in filtered_tasks
                                                        if task.get("dbd_details")
                                                    ]
                                                    if details_list:
                                                        st.success(f"✅ พบข้อมูล DBD สำหรับเลขทะเบียน {normalized_selected}")
                                                        details_df = pd.DataFrame(details_list)
                                                        st.dataframe(details_df, use_container_width=True)
                                                        peak_log(
                                                            f"✅ ดึงข้อมูล DBD แปลงเป็นตารางสำเร็จสำหรับเลขทะเบียน {normalized_selected}",
                                                            "success",
                                                        )
                                                peak_log(
                                                    f"ℹ️ กำลังประมวลผลเฉพาะเลขทะเบียน {normalized_selected} ({len(filtered_tasks)} รายการ)",
                                                    "info",
                                                )
                                                tasks = filtered_tasks
                                            else:
                                                st.warning("⚠️ กรุณาเลือกเลขทะเบียนที่จะกรอกก่อนเริ่มทำงาน")
                                                peak_log("⚠️ ยังไม่ได้เลือกเลขทะเบียนที่จะกรอก", "warning")
                                                return

                                        preview_df = pd.DataFrame(tasks)
                                        skipped_df = pd.DataFrame(skipped_records)

                                        with st.expander("🗂 รายการที่เตรียมกรอก (New Peak)", expanded=True):
                                            if preview_df.empty:
                                                st.info("ไม่มีรายการที่ผ่านเงื่อนไขสำหรับ New Peak")
                                            else:
                                                preview_columns = [
                                                    "row_number",
                                                    "amount",
                                                    "transfer_type",
                                                    "dbd_has_data",
                                                    "registration",
                                                ]
                                                if "company_name" in preview_df.columns:
                                                    preview_columns.append("company_name")
                                                preview_columns.append("target_url")
                                                available_preview_columns = [
                                                    col for col in preview_columns if col in preview_df.columns
                                                ]
                                                st.dataframe(
                                                    preview_df[available_preview_columns],
                                                    use_container_width=True,
                                                )
                                        if not skipped_df.empty:
                                            with st.expander("⚠️ รายการที่ถูกข้าม (New Peak)", expanded=False):
                                                st.dataframe(skipped_df, use_container_width=True)

                                        peak_log("🚀 เริ่มประมวลผลข้อมูล Excel สำหรับ New Peak...", "info")
                                        result = newpeak_bot.process_excel_transactions(
                                            df_source.copy(),
                                            amount_column=amount_col,
                                            type_column=type_col,
                                            dbd_column=dbd_col,
                                            company_column=company_col,
                                            log_callback=peak_log,
                                            prepared_tasks=tasks,
                                            skipped_info=skipped_records,
                                        )
                                        if "error" in result:
                                            st.error(f"❌ ไม่สามารถประมวลผลได้: {result['error']}")
                                        else:
                                            st.success("✅ ประมวลผลข้อมูลสำหรับ New Peak สำเร็จ")
                                            col_np1, col_np2, col_np3 = st.columns(3)
                                            with col_np1:
                                                st.metric("รายการที่นำทางสำเร็จ", result.get("processed", 0))
                                            with col_np2:
                                                st.metric("รายการที่ข้าม", result.get("skipped", 0))
                                            with col_np3:
                                                st.metric("ข้อผิดพลาด", len(result.get("errors", [])))

                                            skipped_details = result.get("skipped_details", [])
                                            if skipped_details:
                                                with st.expander("ℹ️ รายการที่ถูกข้ามจากเงื่อนไข", expanded=False):
                                                    st.dataframe(
                                                        pd.DataFrame(skipped_details),
                                                        use_container_width=True,
                                                    )
                                            if result.get("errors"):
                                                st.warning("⚠️ รายการที่เกิดข้อผิดพลาด")
                                                st.dataframe(pd.DataFrame(result["errors"]), use_container_width=True)
                                except Exception as exc:
                                    st.error(f"❌ เกิดข้อผิดพลาดระหว่างประมวลผล New Peak: {exc}")
                                    peak_log(f"❌ เกิดข้อผิดพลาดระหว่างประมวลผล New Peak: {exc}", "error")
                    else:
                        st.error("❌ ไม่สามารถเริ่มการทำงานของ NewPeakBot ได้ กรุณาตรวจสอบ log และไฟล์ config.py")

def main():
    st.title("🏦 โปรแกรมแปลงไฟล์ PDF ธนาคารเป็น Excel")
    st.markdown("---")
    
    # สร้างอินสแตนซ์ของ BankPDFReader
    reader = BankPDFReader()
    
    # Sidebar สำหรับการตั้งค่า
    st.sidebar.header("⚙️ การตั้งค่า")
    
    # เลือกหน้า
    st.sidebar.subheader("📑 เลือกหน้า")
    page = st.sidebar.radio(
        "เลือกหน้าที่ต้องการใช้งาน:",
        ["📄 Statement", "🤖 Bot ดึงข้อมูลกรมพัฒน์", "🧾 Bot รันเปิดใบเสร็จ"],
        help="เลือกหน้าที่ต้องการใช้งาน"
    )
    
    st.sidebar.markdown("---")
    
    # ปุ่มเปิดหน้าเว็บ PeakEngine
    st.sidebar.subheader("🌐 เปิดหน้าเว็บ")
    if st.sidebar.button("🔐 เปิดหน้า PeakEngine Login", use_container_width=True, help="เปิดหน้าเว็บ PeakEngine Login, กรอก username/password, คลิกปุ่ม Login, คลิกปุ่ม PEAK (Deprecated) และ navigate ไปที่ลิงค์อัตโนมัติจาก config.py", key="open_peakengine_btn"):
        with st.sidebar:
            with st.spinner("กำลังเปิดหน้าเว็บ PeakEngine, กรอกข้อมูล, คลิกปุ่ม Login, คลิกปุ่ม PEAK (Deprecated) และ navigate ไปที่ลิงค์..."):
                if open_peakengine_login():
                    st.success("✅ เปิดหน้าเว็บ, กรอกข้อมูล, คลิกปุ่ม Login, คลิกปุ่ม PEAK (Deprecated) และ navigate ไปที่ลิงค์สำเร็จ!")
                    st.info("👀 ตรวจสอบหน้าต่าง Browser ใหม่ที่เปิดขึ้นมา - ระบบจะ login, คลิกปุ่ม PEAK (Deprecated) และ navigate อัตโนมัติ")
                    if config and hasattr(config, 'PEAKENGINE_USERNAME') and config.PEAKENGINE_USERNAME:
                        st.info(f"📧 Username: {config.PEAKENGINE_USERNAME}")
                    if config:
                        if hasattr(config, 'Link_conpany'):
                            st.info(f"🔗 Link_conpany: {config.Link_conpany}")
                        if hasattr(config, 'Link_receipt'):
                            st.info(f"🔗 Link_receipt: {config.Link_receipt}")
                else:
                    st.error("❌ ไม่สามารถเปิดหน้าเว็บ, login, คลิกปุ่ม PEAK (Deprecated) หรือ navigate ได้")
    if st.sidebar.button("🆕 เปิดหน้า New Peak Login", use_container_width=True, help="ทดสอบการเข้าสู่ระบบ https://secure.peakaccount.com ด้วย NewPeakBot และ navigate ตาม config.py", key="open_newpeak_btn"):
        with st.sidebar:
            with st.spinner("กำลังเปิดหน้าเว็บ New Peak, กรอกข้อมูล และนำทางไปยังลิงก์..."):
                if open_newpeak_login():
                    st.success("✅ สั่งเปิด Browser สำหรับระบบ New Peak สำเร็จ! ดูหน้าต่างที่เปิดขึ้นมาเพื่อดูการทำงานของบอท")
                    if config:
                        username = getattr(config, "NEWPEAK_USERNAME", getattr(config, "PEAKENGINE_USERNAME", ""))
                        if username:
                            st.info(f"📧 Username: {username}")
                        if hasattr(config, "Link_compay_newpeak"):
                            st.info(f"🔗 Link_compay_newpeak: {getattr(config, 'Link_compay_newpeak')}")
                        if hasattr(config, "Link_receipt_newpeak"):
                            st.info(f"🔗 Link_receipt_newpeak: {getattr(config, 'Link_receipt_newpeak')}")
                else:
                    st.error("❌ ไม่สามารถเริ่มการทำงานของ NewPeakBot ได้ กรุณาตรวจสอบ log และไฟล์ config.py")
    
    st.sidebar.markdown("---")
    
    # การตั้งค่าบอท DBD (แสดงเมื่ออยู่ในหน้าบอท)
    use_browser_mode = False
    if "Bot" in page:
        st.sidebar.subheader("🤖 การตั้งค่าบอท DBD")
        use_browser_mode = st.sidebar.checkbox(
            "🌐 ใช้ Playwright Browser (แสดงหน้าจอ)",
            value=True,  # เปิด browser แสดงหน้าจอเป็นค่าเริ่มต้น
                help="เปิด Playwright Chromium Browser เพื่อดูการทำงานแบบเรียลไทม์",
                key="use_browser_checkbox"
        )
    
        # เก็บค่าใน session state
        st.session_state['use_browser_mode'] = use_browser_mode
        st.session_state['headless_mode'] = False  # บังคับให้แสดง browser เสมอ
    
    if use_browser_mode:
        st.sidebar.success("👀 **Browser จะเปิดขึ้นมาแสดงการทำงานแบบเรียลไทม์!**")
        st.sidebar.info("💡 **คำแนะนำ:** ดู Browser window ที่จะเปิดขึ้นมา - จะเห็นทุกขั้นตอนที่บอททำงาน")
        st.sidebar.markdown("""
        **ขั้นตอนที่คุณจะเห็น:**
        1. 🌐 เปิด Browser
        2. 📍 เข้าหน้าเว็บ DBD DataWarehouse
        3. 🔍 ค้นหาช่องกรอกข้อมูล
        4. ⌨️ พิมพ์ชื่อบริษัท
        5. 🔘 คลิกปุ่มค้นหา
        6. 📊 แสดงผลลัพธ์
        """)

        if st.sidebar.button("🧪 ทดสอบการเปิด Playwright Browser", help="เปิด Chromium ผ่านคำสั่ง playwright เพื่อยืนยันว่าสามารถใช้งานได้", key="test_browser_btn"):
            with st.sidebar:
                with st.spinner("กำลังสั่งเปิด Playwright Chromium..."):
                    if test_playwright_browser():
                        st.success("✅ สั่งเปิด Playwright แล้ว!")
                        st.info("👀 ตรวจสอบหน้าต่าง Chromium ใหม่ที่เปิดขึ้นมา")
                    else:
                        st.error("❌ การทดสอบ Playwright ไม่สำเร็จ")
                        st.sidebar.markdown("---")
    
    # เลือกธนาคาร (แสดงเมื่ออยู่ในหน้า Statement)
    if page == "📄 Statement":
        st.sidebar.subheader("🏦 การตั้งค่า PDF")
        bank_names = list(reader.bank_configs.keys())
        selected_bank = st.sidebar.selectbox(
            "เลือกธนาคาร:",
            bank_names,
                help="เลือกธนาคารที่ตรงกับไฟล์ PDF ของคุณ",
                key="bank_select"
            )
    else:
        selected_bank = None
    
    # แสดงหน้าแต่ละหน้า
    if page == "📄 Statement":
        uploaded_file, selected_bank = render_statement_page(reader, selected_bank)
    elif page == "🤖 Bot ดึงข้อมูลกรมพัฒน์":
        render_dbd_bot_page(reader)
        uploaded_file = None
    elif page == "🧾 Bot รันเปิดใบเสร็จ":
        render_receipt_bot_page()
        uploaded_file = None
    
    # Footer
    st.markdown("---")
    st.header("ℹ️ ข้อมูลเกี่ยวกับโปรแกรม")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏦 ธนาคารที่รองรับ")
        bank_names = list(reader.bank_configs.keys())
        for bank in bank_names:
            st.write(f"• {bank}")
    
    with col2:
        st.subheader("✨ ฟีเจอร์")
        features = [
            "อ่านไฟล์ PDF อัตโนมัติ",
            "แปลงข้อมูลเป็น Excel",
            "รองรับธนาคารหลายแห่ง",
            "ตรวจสอบธนาคารอัตโนมัติ",
            "แสดงสถิติข้อมูล",
            "UI ที่ใช้งานง่าย"
        ]
        for feature in features:
            st.write(f"• {feature}")
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>🏦 Bank PDF to Excel Converter | พัฒนาด้วย Streamlit</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
        
        
