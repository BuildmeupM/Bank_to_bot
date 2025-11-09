import streamlit as st
import pandas as pd
import sys
import os
import inspect
import importlib.util
from datetime import datetime
import io
import time

# เพิ่ม path โปรเจ็กต์เพื่อให้ import bot_data ได้
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Reload module เพื่อให้แน่ใจว่าใช้โค้ดล่าสุด
# ลบโมดูลเก่าออกเพื่อบังคับให้โหลดใหม่
modules_to_remove = [key for key in sys.modules.keys() if 'bot_data' in key.lower()]
for module_name in modules_to_remove:
    try:
        del sys.modules[module_name]
    except:
        pass

# ลบ __pycache__ เพื่อบังคับให้โหลดโค้ดใหม่
import shutil
cache_dirs = [
    os.path.join(current_dir, '__pycache__'),
    os.path.join(current_dir, '.pytest_cache')
]
for cache_dir in cache_dirs:
    if os.path.exists(cache_dir):
        try:
            shutil.rmtree(cache_dir, ignore_errors=True)
        except:
            pass

# Import โดยระบุ path โดยตรงเพื่อหลีกเลี่ยงปัญหา case-sensitivity และ network drive
try:
    bot_data_path = os.path.join(current_dir, 'bot_data.py')
    if not os.path.exists(bot_data_path):
        # ลองหาไฟล์ที่ชื่อคล้ายกัน (case-insensitive)
        for file in os.listdir(current_dir):
            if file.lower() == 'bot_data.py':
                bot_data_path = os.path.join(current_dir, file)
                break
    
    if os.path.exists(bot_data_path):
        # ใช้ timestamp เป็น module name เพื่อบังคับให้โหลดใหม่ทุกครั้ง
        module_name = f"bot_data_module_{int(time.time() * 1000)}"
        spec = importlib.util.spec_from_file_location(module_name, bot_data_path)
        bot_data_module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = bot_data_module
        spec.loader.exec_module(bot_data_module)
        DBDDataWarehouseBot = bot_data_module.DBDDataWarehouseBot
        create_dbd_summary_table = bot_data_module.create_dbd_summary_table
    else:
        # Fallback to normal import แต่ลบ cache ก่อน
        if 'bot_data' in sys.modules:
            del sys.modules['bot_data']
        import importlib
        bot_data = importlib.import_module('bot_data')
        importlib.reload(bot_data)
        DBDDataWarehouseBot = bot_data.DBDDataWarehouseBot
        create_dbd_summary_table = bot_data.create_dbd_summary_table
except ImportError as e:
    st.error(f"❌ Error: ไม่สามารถ import bot_data ได้: {str(e)}")
    st.error(f"โปรดตรวจสอบว่าไฟล์ bot_data.py อยู่ในโฟลเดอร์: {current_dir}")
    st.stop()
except Exception as e:
    st.error(f"❌ Error: เกิดข้อผิดพลาดในการโหลดโมดูล: {str(e)}")
    st.error(f"Path ที่ลองหา: {bot_data_path if 'bot_data_path' in locals() else 'ไม่พบ'}")
    st.stop()

# ตรวจสอบว่า class มี use_browser parameter หรือไม่
try:
    sig = inspect.signature(DBDDataWarehouseBot.__init__)
    params = list(sig.parameters.keys())
    if 'use_browser' not in params:
        st.error(f"❌ Error: bot_data module ไม่มี use_browser parameter")
        st.error(f"Parameters ที่พบ: {params}")
        st.error("โปรดตรวจสอบว่าไฟล์ bot_data.py มี use_browser parameter ใน __init__")
        st.stop()
except Exception as e:
    st.error(f"❌ Error: ไม่สามารถตรวจสอบ bot_data module ได้: {str(e)}")
    st.stop()

# ตั้งค่า page
st.set_page_config(
    page_title="DBD DataWarehouse Bot",
    page_icon="🏢",
    layout="wide"
)

st.title("🏢 DBD DataWarehouse Bot")
st.markdown("---")

# Sidebar - เลือกโหมดการเข้าถึง
use_browser_mode = st.sidebar.checkbox(
    "🌐 ใช้ Chromium Browser",
    value=False,
    help="ใช้ Chromium browser จริงแทน requests library เพื่อหลีกเลี่ยงปัญหา 403 Forbidden"
)

headless_mode = st.sidebar.checkbox(
    "🙈 Headless Mode (ซ่อนหน้าจอ)",
    value=False,
    help="เปิด browser แบบ headless (ไม่แสดงหน้าจอ) - เปิดเท่านั้นเมื่อใช้ Browser Mode\n\n⚠️ แนะนำให้ปล่อยว่างเพื่อดูการทำงานแบบเรียลไทม์",
    disabled=not use_browser_mode
)

# Warning ถ้าเลือก headless
if use_browser_mode and headless_mode:
    st.sidebar.warning("⚠️ Headless Mode เปิดอยู่ - จะไม่เห็น browser ทำงาน")

# สร้างอินสแตนซ์ของ bot
if use_browser_mode:
    if headless_mode:
        st.sidebar.info("🌐 ใช้ Chromium Browser Mode (Headless)\n\nBrowser จะทำงานแบบซ่อนหน้าจอ")
    else:
        st.sidebar.info("🌐 ใช้ Chromium Browser Mode (แสดงหน้าจอ)\n\n👀 จะเปิด Chromium browser ให้เห็นการทำงานแบบเรียลไทม์")
        st.sidebar.success("💡 **เคล็ดลับ:** ตรวจสอบ Chromium window ที่เปิดอยู่เพื่อดูการทำงานแบบเรียลไทม์")
    
    bot = DBDDataWarehouseBot(use_browser=True, headless=headless_mode)
else:
    st.sidebar.info("📡 ใช้ Requests Mode\n\nใช้ requests library ธรรมดา (เร็วกว่าแต่เสี่ยงได้ 403)")
    bot = DBDDataWarehouseBot(use_browser=False)

# Sidebar
st.sidebar.header("⚙️ การตั้งค่า")
st.sidebar.info("""
**เกี่ยวกับโปรแกรม:**
- ดึงข้อมูลบริษัทจาก DBD DataWarehouse
- รองรับการค้นหาชื่อบริษัท
- สามารถประมวลผลไฟล์ Excel
""")

# เลือกโหมดการใช้งาน
mode = st.sidebar.radio(
    "เลือกโหมด:",
    ["🔍 ค้นหาบริษัทเดี่ยว", "📊 อัปโหลดไฟล์ Excel"],
    help="เลือกวิธีการใช้งานโปรแกรม"
)

if mode == "🔍 ค้นหาบริษัทเดี่ยว":
    st.subheader("🔍 ค้นหาข้อมูลบริษัทจาก DBD DataWarehouse")
    
    # ช่องค้นหา
    company_name = st.text_input(
        "ชื่อบริษัท/บุคคล:",
        placeholder="ตัวอย่าง: ทรอเวลล์ กร",
        help="กรอกชื่อบริษัทหรือบุคคลที่ต้องการค้นหา"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        search_button = st.button("🔍 ค้นหา", type="primary", use_container_width=True)
    
    with col2:
        if st.button("🧹 ล้างข้อมูล", use_container_width=True):
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
            
            # ค้นหาข้อมูล (พร้อม log callback)
            company_info = bot.search_company_info(company_name, log_callback=log_callback)
            
            if "error" in company_info:
                st.error(f"❌ {company_info['error']}")
            else:
                # แสดงผลลัพธ์
                st.success("✅ พบข้อมูลบริษัท!")
                
                # แสดงข้อมูลในรูปแบบตาราง
                directors_display = " | ".join(company_info.get("directors_list", [])) if company_info.get("directors_list") else company_info.get("directors", "-")

                info_data = {
                    "รายการ": [
                        "ชื่อบริษัท",
                        "เลขทะเบียน",
                        "ประเภทธุรกิจ",
                        "สถานะ",
                        "ทุนจดทะเบียน",
                        "ที่อยู่",
                        "โทรศัพท์",
                        "อีเมล",
                        "วันที่จดทะเบียน",
                        "วันที่อัปเดต",
                        "รายชื่อกรรมการ"
                    ],
                    "ข้อมูล": [
                        company_info.get("company_name", "-"),
                        company_info.get("registration_number", "-"),
                        company_info.get("business_type", "-"),
                        company_info.get("status", "-"),
                        company_info.get("registered_capital", "-"),
                        company_info.get("address", "-"),
                        company_info.get("phone", "-"),
                        company_info.get("email", "-"),
                        company_info.get("found_date", "-"),
                        company_info.get("last_update", "-"),
                        directors_display or "-"
                    ]
                }
                
                df_result = pd.DataFrame(info_data)
                
                # แสดงข้อมูล
                st.subheader("📋 ข้อมูลบริษัท")
                st.dataframe(df_result, use_container_width=True, hide_index=True)
                
                # แสดงข้อมูลแบบฟอร์ม
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### 📌 ข้อมูลพื้นฐาน")
                    if company_info.get("registration_number"):
                        st.write(f"**เลขทะเบียน:** {company_info['registration_number']}")
                    if company_info.get("business_type"):
                        st.write(f"**ประเภทธุรกิจ:** {company_info['business_type']}")
                    if company_info.get("status"):
                        st.write(f"**สถานะ:** {company_info['status']}")
                    if company_info.get("registered_capital"):
                        st.write(f"**ทุนจดทะเบียน:** {company_info['registered_capital']}")
                
                with col2:
                    st.markdown("### 📍 ข้อมูลติดต่อ")
                    if company_info.get("address"):
                        st.write(f"**ที่อยู่:** {company_info['address']}")
                    if company_info.get("phone"):
                        st.write(f"**โทรศัพท์:** {company_info['phone']}")
                    if company_info.get("email"):
                        st.write(f"**อีเมล:** {company_info['email']}")
                    if company_info.get("found_date"):
                        st.write(f"**วันที่จดทะเบียน:** {company_info['found_date']}")
                    directors_list = company_info.get("directors_list", [])
                    if directors_list:
                        st.markdown("**รายชื่อกรรมการ:**")
                        for director in directors_list:
                            st.markdown(f"- {director}")
                    elif company_info.get("directors"):
                        st.write(f"**รายชื่อกรรมการ:** {company_info['directors']}")
                
                # แสดงข้อมูลที่จัดรูปแบบแล้ว
                formatted_info = bot.format_company_info(company_info)
                st.markdown("### 📄 ข้อมูลที่จัดรูปแบบ")
                st.info(formatted_info)

elif mode == "📊 อัปโหลดไฟล์ Excel":
    st.subheader("📊 อัปโหลดไฟล์ Excel")
    
    uploaded_file = st.file_uploader(
        "เลือกไฟล์ Excel",
        type=['xlsx', 'xls'],
        help="อัปโหลดไฟล์ Excel ที่มีคอลัมน์ชื่อบริษัท/บุคคล"
    )
    
    if uploaded_file is not None:
        try:
            # อ่านไฟล์ Excel จากชีตที่กำหนด
            df = pd.read_excel(uploaded_file, sheet_name='ข้อมูลจำแนกแล้ว')
            
            st.success("✅ อัปโหลดไฟล์ Excel สำเร็จ!")
            
            # แสดงข้อมูลตัวอย่าง
            st.subheader("📊 ข้อมูลตัวอย่าง")
            st.write(f"**จำนวนแถว:** {len(df)}")
            st.write(f"**จำนวนคอลัมน์:** {len(df.columns)}")
            
            # แสดงคอลัมน์ที่มีอยู่
            st.write("**คอลัมน์ที่มีอยู่:**")
            st.write(", ".join(df.columns.tolist()))
            
            # ตรวจหาคอลัมน์ประเภทผู้ส่งโอน
            type_column_candidates = [col for col in df.columns if 'ประเภท' in str(col) and ('ผู้ส่งโอน' in str(col) or 'ผู้ส่ง' in str(col))]
            if not type_column_candidates:
                st.error("❌ ไม่พบคอลัมน์ประเภทผู้ส่งโอนในชีต 'ข้อมูลจำแนกแล้ว' โปรดตรวจสอบไฟล์")
                st.stop()

            type_column = type_column_candidates[0]
            st.info(f"🔍 ใช้คอลัมน์ '{type_column}' สำหรับจำแนกประเภทผู้ส่งโอน")

            # เลือกคอลัมน์ที่มีชื่อบริษัท
            company_columns = [col for col in df.columns if any(keyword in str(col).lower() for keyword in ['บริษัท', 'ชื่อ', 'company', 'name'])]
            
            if company_columns:
                selected_column = st.selectbox(
                    "เลือกคอลัมน์ที่มีชื่อบริษัท/บุคคล:",
                    company_columns,
                    help="เลือกคอลัมน์ที่มีชื่อบริษัทหรือบุคคล"
                )
                
                if selected_column:
                    # แสดงข้อมูลตัวอย่าง
                    st.subheader("📋 ข้อมูลตัวอย่าง")
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    # ตั้งค่าการประมวลผล
                    st.subheader("⚙️ ตั้งค่าการประมวลผล")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        delay = st.slider(
                            "หน่วงเวลาระหว่างการค้นหา (วินาที):",
                            min_value=0.5,
                            max_value=5.0,
                            value=1.0,
                            step=0.5,
                            help="หน่วงเวลาเพื่อไม่ให้โหลดเซิร์ฟเวอร์หนักเกินไป"
                        )
                    
                    with col2:
                        show_logs = st.checkbox("แสดงขั้นตอนการทำงาน", value=True)
                    
                    eligible_types_preview = {"บริษัท (บจก.)", "ห้างหุ้นส่วน (หจก.)"}
                    type_series_preview = df[type_column].astype(str).str.strip()
                    raw_name_series_preview = df[selected_column]
                    name_mask_preview = raw_name_series_preview.notna() & raw_name_series_preview.astype(str).str.strip().ne("")
                    eligible_preview_count = int((type_series_preview.isin(eligible_types_preview) & name_mask_preview).sum())
                    st.caption(f"รายการที่จะประมวลผล (บริษัท (บจก.) และ ห้างหุ้นส่วน (หจก.)): {eligible_preview_count}")

                    # ปุ่มประมวลผล
                    if st.button("🚀 เริ่มประมวลผล", type="primary", use_container_width=True):
                        # แสดงคำเตือนถ้าใช้ browser mode
                        if use_browser_mode and not headless_mode:
                            st.info("👀 **ดู Chromium Browser ที่เปิดอยู่** - จะเห็นการทำงานของทุกบริษัทแบบเรียลไทม์!")
                        
                        # สร้างคอลัมน์ใหม่สำหรับข้อมูล DBD
                        df['ข้อมูล DBD'] = ""
                        df['ชื่อบริษัทจาก DBD'] = ""
                        df['รายชื่อกรรมการ'] = ""
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

                        # กรองเฉพาะรายการที่ต้องประมวลผล (บริษัท (บจก.) และ ห้างหุ้นส่วน (หจก.))
                        eligible_types = {"บริษัท (บจก.)", "ห้างหุ้นส่วน (หจก.)"}
                        type_series = df[type_column].astype(str).str.strip()
                        eligible_mask = type_series.isin(eligible_types)

                        if eligible_mask.sum() == 0:
                            st.warning("⚠️ ไม่พบรายการประเภท บริษัท (บจก.) หรือ ห้างหุ้นส่วน (หจก.) สำหรับประมวลผล")
                            st.stop()

                        raw_name_series = df[selected_column]
                        name_mask = raw_name_series.notna() & raw_name_series.astype(str).str.strip().ne("")
                        eligible_indices = df[eligible_mask & name_mask].index.tolist()

                        if not eligible_indices:
                            st.warning("⚠️ ไม่พบชื่อบริษัท/บุคคลสำหรับรายการที่เป็น บริษัท (บจก.) หรือ ห้างหุ้นส่วน (หจก.)")
                            st.stop()
                        
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
                        log_expander = None
                        log_messages = []
                        log_placeholder = None
                        
                        if show_logs:
                            with log_container:
                                st.subheader("📋 ขั้นตอนการทำงานของบอท")
                                log_expander = st.expander("🔍 ดูขั้นตอนการทำงานแบบละเอียด", expanded=False)
                                log_placeholder = log_expander.empty()
                        
                        def log_callback(message, status="info"):
                            """Callback สำหรับแสดง log"""
                            if show_logs and log_expander:
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
                        
                        # ดึงข้อมูลสำหรับแต่ละบริษัท
                        total_companies = len(eligible_indices)
                        processed_count = 0
                        success_stats = 0
                        error_stats = 0
                        not_found_stats = 0
                        
                        for index in eligible_indices:
                            row = df.loc[index]
                            company_name = row[selected_column]
                            
                            # อัปเดต status
                            processed_count += 1
                            progress = processed_count / total_companies
                            progress_bar.progress(progress)
                            
                            status_text.text(f"กำลังประมวลผล {processed_count}/{total_companies}: {company_name}")
                            
                            # ค้นหาข้อมูลบริษัท (พร้อม log callback)
                            company_info = bot.search_company_info(str(company_name), log_callback=log_callback if show_logs else None)
                            
                            # จัดรูปแบบข้อมูลสำหรับใส่ในคอลัมน์
                            formatted_info = bot.format_company_info(company_info)
                            df.at[index, 'ข้อมูล DBD'] = formatted_info

                            if isinstance(company_info, dict):
                                if company_info.get("directors_list"):
                                    directors_value = " | ".join(company_info.get("directors_list", []))
                                else:
                                    directors_value = company_info.get("directors", "")
                                df.at[index, 'รายชื่อกรรมการ'] = directors_value

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
                            time.sleep(delay)
                        
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
                        
                        # แสดงตารางสรุปข้อมูล DBD
                        dbd_summary = create_dbd_summary_table(df)
                        
                        if not dbd_summary.empty:
                            st.subheader("📋 สรุปข้อมูล DBD")
                            st.dataframe(dbd_summary, use_container_width=True)
                        
                        # สร้างไฟล์ Excel พร้อมข้อมูล DBD
                        output_dbd = io.BytesIO()
                        with pd.ExcelWriter(output_dbd, engine='openpyxl') as writer:
                            df.to_excel(writer, sheet_name='ข้อมูลพร้อม DBD', index=False)
                            
                            if not dbd_summary.empty:
                                dbd_summary.to_excel(writer, sheet_name='สรุปข้อมูล DBD', index=False)
                        
                        output_dbd.seek(0)
                        
                        st.download_button(
                            label="📥 ดาวน์โหลดข้อมูลพร้อม DBD",
                            data=output_dbd.getvalue(),
                            file_name=f"excel_with_dbd_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            help="ข้อมูล Excel พร้อมข้อมูลจาก DBD DataWarehouse"
                        )
                        
                        # แสดงข้อมูลที่ประมวลผลแล้ว
                        st.subheader("📊 ข้อมูลที่ประมวลผลแล้ว")
                        st.dataframe(df, use_container_width=True)
            else:
                st.warning("⚠️ ไม่พบคอลัมน์ที่มีชื่อบริษัท/บุคคล")
                st.write("**คอลัมน์ที่มีอยู่:**")
                for col in df.columns:
                    st.write(f"• {col}")
        
        except Exception as e:
            st.error(f"❌ เกิดข้อผิดพลาดในการอ่านไฟล์ Excel: {str(e)}")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
        <p>🏢 DBD DataWarehouse Bot | พัฒนาด้วย Streamlit</p>
    </div>
    """,
    unsafe_allow_html=True
)
