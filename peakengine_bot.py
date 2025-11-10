import pandas as pd
import time
import re
from typing import Dict, List, Optional, Callable, Any
import logging
from concurrent.futures import ThreadPoolExecutor
import asyncio
from datetime import datetime

# ตั้งค่า logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PeakEngineBot:
    """คลาสสำหรับทำงานอัตโนมัติบน PeakEngine"""
    
    def __init__(self, use_browser: bool = True, headless: bool = False):
        """
        Initialize bot
        
        Args:
            use_browser (bool): ใช้ browser (Playwright) แทน requests
            headless (bool): เปิด browser แบบ headless (ซ่อนหน้าจอ)
        """
        self.base_url = "https://secure.peakengine.com"
        self.login_url = f"{self.base_url}/Home/Login"
        self.use_browser = use_browser
        self.headless = headless
        self.browser = None
        self.page = None
        self.playwright = None
        self._executor = None
        self.is_logged_in = False
        
        if use_browser:
            try:
                from playwright.async_api import async_playwright
                
                # ใช้ ThreadPoolExecutor เพื่อหลีกเลี่ยงปัญหา event loop ใน Streamlit
                self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="playwright")
                
                def init_playwright_in_new_event_loop():
                    """สร้าง event loop ใหม่ใน thread เพื่อหลีกเลี่ยงปัญหา Streamlit"""
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        async def async_init():
                            logger.info("🚀 กำลังเปิด Playwright Browser...")
                            pw = await async_playwright().start()
                            
                            logger.info("🌐 กำลัง launch Chromium browser...")
                            browser = await pw.chromium.launch(
                                headless=headless,
                                args=[
                                    '--disable-blink-features=AutomationControlled',
                                    '--disable-dev-shm-usage',
                                    '--no-sandbox',
                                    '--start-maximized'
                                ]
                            )
                            
                            logger.info("📄 กำลังสร้าง browser context...")
                            context = await browser.new_context(
                                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                                viewport={'width': 1920, 'height': 1080},
                                screen={'width': 1920, 'height': 1080}
                            )
                            
                            logger.info("🆕 กำลังสร้าง new page...")
                            page = await context.new_page()
                            
                            logger.info("✅ เปิด Playwright Browser สำเร็จ!")
                            return pw, browser, page
                        
                        return loop.run_until_complete(async_init())
                    finally:
                        pass
                
                logger.info("🚀 กำลังเริ่มต้น Playwright Browser ใน thread แยก...")
                
                # รันใน thread แยก
                future = self._executor.submit(init_playwright_in_new_event_loop)
                self.playwright, self.browser, self.page = future.result(timeout=60)
                
                logger.info("✅ เปิด Playwright Browser สำเร็จ!")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ ไม่สามารถเปิด Playwright Browser ได้: {error_msg}")
                raise Exception(f"ไม่สามารถเปิด Browser ได้: {error_msg}\n\n💡 ตรวจสอบ:\n1. Playwright ติดตั้งแล้ว: pip install playwright\n2. Browser binaries ติดตั้งแล้ว: playwright install chromium")
    
    def open_login_page_and_fill(self, username: str, password: str, link_company: Optional[str] = None, link_receipt: Optional[str] = None, log_callback: Optional[Callable] = None) -> bool:
        """
        เปิดหน้า Login, กรอก username/password, คลิกปุ่ม Login, คลิกปุ่ม PEAK (Deprecated) และ navigate ไปที่ Link_conpany และ Link_receipt
        
        Args:
            username (str): Username สำหรับ login
            password (str): Password สำหรับ login
            link_company (Optional[str]): URL สำหรับ navigate ไปที่ Link_conpany
            link_receipt (Optional[str]): URL สำหรับ navigate ไปที่ Link_receipt
            log_callback (Optional[Callable]): ฟังก์ชันสำหรับแสดง log (message, status)
            
        Returns:
            bool: True ถ้ากรอกข้อมูล, คลิกปุ่ม Login, คลิกปุ่ม PEAK (Deprecated) และ navigate สำเร็จ, False ถ้าไม่สำเร็จ
        """
        def log(message: str, status: str = "info"):
            """Helper function สำหรับ log"""
            if log_callback:
                try:
                    log_callback(message, status)
                except:
                    pass
            logger.info(message)
        
        if not self.use_browser or not self.page:
            log("❌ Browser ยังไม่ได้เปิด", "error")
            return False
        
        # ถ้ายังไม่มีลิงค์ ให้ลองอ่านจาก config
        if not link_company or not link_receipt:
            try:
                import config
                if not link_company:
                    link_company = getattr(config, 'Link_conpany', None)
                    if link_company:
                        log(f"📖 อ่าน Link_conpany จาก config: {link_company}", "info")
                    else:
                        log("⚠️ ไม่พบ Link_conpany ใน config.py", "warning")
                        # Debug: ตรวจสอบว่ามี attribute อะไรบ้าง
                        try:
                            attrs = [attr for attr in dir(config) if not attr.startswith('_')]
                            log(f"🔍 Attributes ใน config: {', '.join(attrs)}", "debug")
                        except:
                            pass
                if not link_receipt:
                    link_receipt = getattr(config, 'Link_receipt', None)
                    if link_receipt:
                        log(f"📖 อ่าน Link_receipt จาก config: {link_receipt}", "info")
                    else:
                        log("⚠️ ไม่พบ Link_receipt ใน config.py", "warning")
            except ImportError:
                log("⚠️ ไม่สามารถ import config ได้", "warning")
            except Exception as e:
                log(f"⚠️ เกิดข้อผิดพลาดในการอ่าน config: {str(e)}", "warning")
        
        try:
            def fill_async():
                """รัน fill operations ใน thread ด้วย async"""
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_closed():
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                except:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                async def async_fill():
                    try:
                        log("📍 กำลังเข้าหน้า Login...", "info")
                        # ใช้ 'domcontentloaded' แทน 'networkidle' เพื่อให้โหลดเร็วขึ้น
                        # และเพิ่ม timeout เป็น 60 วินาที
                        try:
                            await self.page.goto(self.login_url, wait_until='domcontentloaded', timeout=60000)
                            log("✅ โหลดหน้า Login (domcontentloaded) เสร็จแล้ว", "success")
                        except Exception as e:
                            log(f"⚠️ domcontentloaded timeout, ลอง load แทน: {str(e)[:100]}", "warning")
                            # ถ้า domcontentloaded timeout ลอง load แทน
                            await self.page.goto(self.login_url, wait_until='load', timeout=60000)
                            log("✅ โหลดหน้า Login (load) เสร็จแล้ว", "success")
                        
                        # รอให้หน้าเว็บโหลดเสร็จและ JavaScript ทำงาน (ลดเวลาเพื่อเพิ่มความเร็ว)
                        await asyncio.sleep(0.2)
                        
                        # รอให้ input fields ปรากฏ (ลด timeout เพื่อเพิ่มความเร็ว)
                        log("🔍 กำลังรอให้ input fields ปรากฏ...", "info")
                        fields_found = False
                        try:
                            # ลอง CSS selector ก่อน
                            await self.page.wait_for_selector('#usernametxt', timeout=3000, state='visible')
                            log("✅ พบ input fields บนหน้าเว็บ (CSS selector)", "success")
                            fields_found = True
                        except Exception as e:
                            log(f"⚠️ ไม่พบด้วย CSS selector: {str(e)[:100]}", "warning")
                            try:
                                # ลอง XPath
                                username_locator = self.page.locator('//*[@id="usernametxt"]')
                                await username_locator.wait_for(state='visible', timeout=3000)
                                log("✅ พบ input fields บนหน้าเว็บ (XPath)", "success")
                                fields_found = True
                            except Exception as e2:
                                log(f"⚠️ ไม่พบด้วย XPath: {str(e2)[:100]}", "warning")
                        
                        if not fields_found:
                            log("⚠️ รอ input fields เพิ่มเติม...", "warning")
                            await asyncio.sleep(0.2)
                        
                        # หาช่องกรอก username
                        log("🔍 กำลังค้นหาช่องกรอก username...", "info")
                        username_input = None
                        
                        # ลอง XPath ก่อน (ตามที่ผู้ใช้ระบุ) - ลด timeout เพื่อเพิ่มความเร็ว
                        try:
                            username_input = self.page.locator('//*[@id="usernametxt"]')
                            await username_input.wait_for(state='visible', timeout=1500)
                            if await username_input.count() > 0:
                                log("✅ พบช่อง username ด้วย XPath: //*[@id=\"usernametxt\"]", "success")
                        except Exception as e:
                            log(f"⚠️ ไม่พบด้วย XPath: {str(e)[:100]}", "warning")
                            username_input = None
                        
                        # ถ้ายังไม่พบ ลอง CSS selector - ลด timeout เพื่อเพิ่มความเร็ว
                        if not username_input or await username_input.count() == 0:
                            username_selectors = [
                                '#usernametxt',
                                'input#usernametxt',
                                'input[id="usernametxt"]',
                                'input[name="username"]',
                                'input[name="email"]',
                                'input[type="text"]',
                                'input#username',
                                'input#email',
                                'input.form-control',
                                'input[placeholder*="username" i]',
                                'input[placeholder*="email" i]'
                            ]
                            
                            for selector in username_selectors:
                                try:
                                    element = await self.page.wait_for_selector(selector, timeout=1000)
                                    if element:
                                        username_input = self.page.locator(selector)
                                        log(f"✅ พบช่อง username ด้วย selector: {selector}", "success")
                                        break
                                except:
                                    continue
                        
                        if not username_input or (hasattr(username_input, 'count') and await username_input.count() == 0):
                            log("❌ ไม่พบช่องกรอก username", "error")
                            # ถ่าย screenshot เพื่อ debug
                            try:
                                await self.page.screenshot(path='peakengine_username_error.png', full_page=True)
                                log("📸 ถ่าย screenshot ไว้ที่: peakengine_username_error.png", "info")
                            except:
                                pass
                            return False
                        
                        # กรอก username (ลด delay เพื่อเพิ่มความเร็ว)
                        try:
                            await username_input.click()
                            await asyncio.sleep(0.05)
                            await username_input.clear()
                            await username_input.fill(username)
                            await asyncio.sleep(0.05)
                            
                            # ตรวจสอบว่ากรอกสำเร็จหรือไม่
                            value = await username_input.input_value()
                            if value == username or username in value:
                                log(f"✅ กรอก username สำเร็จ: {username}", "success")
                            else:
                                log(f"⚠️ กรอก username อาจไม่สำเร็จ (ค่า: {value})", "warning")
                        except Exception as e:
                            log(f"❌ เกิดข้อผิดพลาดในการกรอก username: {str(e)}", "error")
                            return False
                        
                        await asyncio.sleep(0.1)
                        
                        # หาช่องกรอก password
                        log("🔍 กำลังค้นหาช่องกรอก password...", "info")
                        password_input = None
                        
                        # ลอง XPath ก่อน (ตามที่ผู้ใช้ระบุ) - ลด timeout เพื่อเพิ่มความเร็ว
                        try:
                            password_input = self.page.locator('//*[@id="passwordtxt"]')
                            await password_input.wait_for(state='visible', timeout=1500)
                            if await password_input.count() > 0:
                                log("✅ พบช่อง password ด้วย XPath: //*[@id=\"passwordtxt\"]", "success")
                        except Exception as e:
                            log(f"⚠️ ไม่พบด้วย XPath: {str(e)[:100]}", "warning")
                            password_input = None
                        
                        # ถ้ายังไม่พบ ลอง CSS selector - ลด timeout เพื่อเพิ่มความเร็ว
                        if not password_input or await password_input.count() == 0:
                            password_selectors = [
                                '#passwordtxt',
                                'input#passwordtxt',
                                'input[id="passwordtxt"]',
                                'input[name="password"]',
                                'input[type="password"]',
                                'input#password',
                                'input.form-control[type="password"]'
                            ]
                            
                            for selector in password_selectors:
                                try:
                                    element = await self.page.wait_for_selector(selector, timeout=1000)
                                    if element:
                                        password_input = self.page.locator(selector)
                                        log(f"✅ พบช่อง password ด้วย selector: {selector}", "success")
                                        break
                                except:
                                    continue
                        
                        if not password_input or (hasattr(password_input, 'count') and await password_input.count() == 0):
                            log("❌ ไม่พบช่องกรอก password", "error")
                            # ถ่าย screenshot เพื่อ debug
                            try:
                                await self.page.screenshot(path='peakengine_password_error.png', full_page=True)
                                log("📸 ถ่าย screenshot ไว้ที่: peakengine_password_error.png", "info")
                            except:
                                pass
                            return False
                        
                        # กรอก password (ลด delay เพื่อเพิ่มความเร็ว)
                        try:
                            await password_input.click()
                            await asyncio.sleep(0.05)
                            await password_input.clear()
                            await password_input.fill(password)
                            await asyncio.sleep(0.05)
                            
                            # ตรวจสอบว่ากรอกสำเร็จหรือไม่ (password อาจจะไม่แสดงค่า)
                            value = await password_input.input_value()
                            if len(value) > 0:
                                log(f"✅ กรอก password สำเร็จ (ความยาว: {len(value)} ตัวอักษร)", "success")
                            else:
                                # ลองใช้ type แทน (ลด delay)
                                await password_input.type(password, delay=10)
                                await asyncio.sleep(0.1)
                                value = await password_input.input_value()
                                if len(value) > 0:
                                    log(f"✅ กรอก password สำเร็จด้วย type() (ความยาว: {len(value)} ตัวอักษร)", "success")
                                else:
                                    log("⚠️ กรอก password อาจไม่สำเร็จ", "warning")
                        except Exception as e:
                            log(f"❌ เกิดข้อผิดพลาดในการกรอก password: {str(e)}", "error")
                            return False
                        
                        await asyncio.sleep(0.1)
                        
                        # คลิกปุ่ม Login (ลด timeout และ delay เพื่อเพิ่มความเร็ว)
                        log("🔍 กำลังค้นหาปุ่ม Login...", "info")
                        login_button = None
                        
                        # ลองหลายวิธีในการหาปุ่ม Login - ลด timeout เพื่อเพิ่มความเร็ว
                        login_button_selectors = [
                            '#loginbtn',  # ID
                            'div#loginbtn',  # ID with tag
                            '.login-btn',  # Class
                            'div.login-btn',  # Class with tag
                            'div[class*="login-btn"]',  # Class contains
                            '//div[@id="loginbtn"]',  # XPath by ID
                            '//div[contains(@class, "login-btn")]',  # XPath by class
                            '//div[text()="เข้าใช้งาน"]',  # XPath by text
                            'button[type="submit"]',  # Submit button
                            'button:has-text("เข้าใช้งาน")',  # Button with text
                        ]
                        
                        for selector in login_button_selectors:
                            try:
                                if selector.startswith('//'):
                                    # XPath - ลด timeout เพื่อเพิ่มความเร็ว
                                    login_button = self.page.locator(selector)
                                    await login_button.wait_for(state='visible', timeout=1000)
                                    if await login_button.count() > 0:
                                        log(f"✅ พบปุ่ม Login ด้วย XPath: {selector}", "success")
                                        break
                                else:
                                    # CSS selector - ลด timeout เพื่อเพิ่มความเร็ว
                                    element = await self.page.wait_for_selector(selector, timeout=1000, state='visible')
                                    if element:
                                        login_button = self.page.locator(selector)
                                        log(f"✅ พบปุ่ม Login ด้วย selector: {selector}", "success")
                                        break
                            except Exception as e:
                                log(f"⚠️ ไม่พบด้วย selector {selector}: {str(e)[:50]}", "debug")
                                continue
                        
                        if not login_button or (hasattr(login_button, 'count') and await login_button.count() == 0):
                            log("❌ ไม่พบปุ่ม Login", "error")
                            # ถ่าย screenshot เพื่อ debug
                            try:
                                await self.page.screenshot(path='peakengine_login_button_error.png', full_page=True)
                                log("📸 ถ่าย screenshot ไว้ที่: peakengine_login_button_error.png", "info")
                            except:
                                pass
                            return False
                        
                        # คลิกปุ่ม Login (ลด delay เพื่อเพิ่มความเร็ว)
                        try:
                            log("🔘 กำลังคลิกปุ่ม Login...", "info")
                            await login_button.click()
                            await asyncio.sleep(0.5)  # ลดเวลา
                            log("✅ คลิกปุ่ม Login สำเร็จ", "success")
                            
                            # รอให้หน้าเว็บโหลดเสร็จหลังคลิก Login (ไม่รอ networkidle เพื่อความเร็ว)
                            log("⏳ รอให้หน้าเว็บโหลดเสร็จ...", "info")
                            try:
                                await self.page.wait_for_load_state('domcontentloaded', timeout=5000)
                            except:
                                pass
                            await asyncio.sleep(0.2)  # รอเล็กน้อยเพื่อให้หน้าเว็บแสดงผล
                            
                            # ตรวจสอบว่า login สำเร็จหรือไม่ (ดูจาก URL หรือหน้าเว็บ)
                            current_url = self.page.url
                            log(f"📍 URL หลังคลิก Login: {current_url}", "info")
                            
                            # ตรวจสอบว่า login สำเร็จ (URL ไม่มี "login" หรือมี "SelectApplication" หรือ "Home")
                            login_success = (
                                "login" not in current_url.lower() or 
                                "selectapplication" in current_url.lower() or
                                "/home" in current_url.lower() or
                                current_url.endswith("secure.peakengine.com/") or
                                "?emi=" in current_url
                            )
                            
                            if login_success:
                                log("✅ Login สำเร็จ! (URL เปลี่ยนแล้ว)", "success")
                                
                                # รอให้หน้าเว็บแสดงผลเสร็จก่อนคลิกปุ่ม (ไม่รอ networkidle เพื่อความเร็ว)
                                log("⏳ รอให้หน้าเว็บแสดงผลเสร็จ...", "info")
                                try:
                                    await self.page.wait_for_load_state('domcontentloaded', timeout=2000)
                                except:
                                    pass
                                await asyncio.sleep(0.1)  # รอเล็กน้อยเพื่อให้หน้าเว็บแสดงผล
                                
                                # คลิกที่ปุ่ม "PEAK (Deprecated)" ก่อน navigate
                                log("🔍 กำลังค้นหาปุ่ม PEAK (Deprecated)...", "info")
                                back_button = None
                                
                                # ลองหลายวิธีในการหาปุ่ม
                                back_button_selectors = [
                                    '#btnBackToOldPeak',  # ID
                                    'p#btnBackToOldPeak',  # ID with tag
                                    'p[id="btnBackToOldPeak"]',  # ID with attribute
                                    '//p[@id="btnBackToOldPeak"]',  # XPath by ID
                                    '//p[contains(text(), "PEAK (Deprecated)")]',  # XPath by text
                                ]
                                
                                for selector in back_button_selectors:
                                    try:
                                        if selector.startswith('//'):
                                            # XPath
                                            back_button = self.page.locator(selector)
                                            await back_button.wait_for(state='visible', timeout=2000)
                                            if await back_button.count() > 0:
                                                log(f"✅ พบปุ่ม PEAK (Deprecated) ด้วย XPath: {selector}", "success")
                                                break
                                        else:
                                            # CSS selector
                                            element = await self.page.wait_for_selector(selector, timeout=2000, state='visible')
                                            if element:
                                                back_button = self.page.locator(selector)
                                                log(f"✅ พบปุ่ม PEAK (Deprecated) ด้วย selector: {selector}", "success")
                                                break
                                    except Exception as e:
                                        log(f"⚠️ ไม่พบด้วย selector {selector}: {str(e)[:50]}", "debug")
                                        continue
                                
                                if back_button and await back_button.count() > 0:
                                    try:
                                        log("🔘 กำลังคลิกปุ่ม PEAK (Deprecated)...", "info")
                                        await back_button.click()
                                        await asyncio.sleep(0.2)  # รอเล็กน้อยหลังคลิก
                                        log("✅ คลิกปุ่ม PEAK (Deprecated) สำเร็จ", "success")
                                        
                                        # รอให้หน้าเว็บแสดงผลหลังคลิก (ไม่รอ networkidle เพื่อความเร็ว)
                                        try:
                                            await self.page.wait_for_load_state('domcontentloaded', timeout=2000)
                                        except:
                                            pass
                                        await asyncio.sleep(0.1)
                                    except Exception as e:
                                        log(f"⚠️ เกิดข้อผิดพลาดในการคลิกปุ่ม: {str(e)[:100]}", "warning")
                                else:
                                    log("⚠️ ไม่พบปุ่ม PEAK (Deprecated) - ข้ามการคลิก", "warning")
                                
                                # Navigate ไปที่ Link_conpany และ Link_receipt หลังจากคลิกปุ่ม
                                # ใช้ลิงค์ที่อ่านไว้แล้วจาก closure
                                # Navigate ไปที่ Link_conpany
                                if link_company:
                                    try:
                                        log(f"🌐 กำลังไปที่ Link_conpany: {link_company}", "info")
                                        
                                        # Navigate ไปที่ Link_conpany (ใช้ domcontentloaded เพื่อความเร็ว - ไม่รอ networkidle)
                                        try:
                                            await self.page.goto(link_company, wait_until='domcontentloaded', timeout=30000)
                                            log("✅ โหลดหน้า Link_conpany (domcontentloaded) เสร็จแล้ว", "success")
                                        except Exception as e:
                                            log(f"⚠️ domcontentloaded timeout: {str(e)[:100]}", "warning")
                                        
                                        # ตรวจสอบว่า URL เปลี่ยนแล้วหรือยัง (ไม่รอ networkidle เพื่อความเร็ว)
                                        await asyncio.sleep(0.1)  # รอเล็กน้อยเพื่อให้ URL อัปเดต
                                        current_url = self.page.url
                                        log(f"📍 URL ปัจจุบัน: {current_url}", "info")
                                        
                                        if link_company in current_url or current_url.startswith(link_company.split('?')[0]):
                                            log("✅ ไปที่ Link_conpany สำเร็จ (URL ถูกต้อง)", "success")
                                        else:
                                            log(f"⚠️ URL อาจไม่ตรงกับที่ต้องการ (คาดหวัง: {link_company})", "warning")
                                        
                                    except Exception as e:
                                        log(f"⚠️ เกิดข้อผิดพลาดในการ navigate ไปที่ Link_conpany: {str(e)[:100]}", "warning")
                                
                                # Navigate ไปที่ Link_receipt (แยก try-except เพื่อให้ทำงานต่อได้แม้ Link_conpany จะมีปัญหา)
                                log(f"🔍 ตรวจสอบ link_receipt: {repr(link_receipt)}", "info")
                                if link_receipt:
                                    try:
                                        log(f"🌐 กำลังไปที่ Link_receipt: {link_receipt}", "info")
                                        
                                        # Navigate ไปที่ Link_receipt (ใช้ domcontentloaded เพื่อความเร็ว - ไม่รอ networkidle)
                                        try:
                                            await self.page.goto(link_receipt, wait_until='domcontentloaded', timeout=30000)
                                            log("✅ โหลดหน้า Link_receipt (domcontentloaded) เสร็จแล้ว", "success")
                                        except Exception as e:
                                            log(f"⚠️ domcontentloaded timeout: {str(e)[:100]}", "warning")
                                        
                                        # ตรวจสอบว่า URL เปลี่ยนแล้วหรือยัง (ไม่รอ networkidle เพื่อความเร็ว)
                                        await asyncio.sleep(0.1)  # รอเล็กน้อยเพื่อให้ URL อัปเดต
                                        current_url = self.page.url
                                        log(f"📍 URL ปัจจุบัน: {current_url}", "info")
                                        
                                        if link_receipt in current_url or current_url.startswith(link_receipt.split('?')[0]):
                                            log("✅ ไปที่ Link_receipt สำเร็จ (URL ถูกต้อง)", "success")
                                        else:
                                            log(f"⚠️ URL อาจไม่ตรงกับที่ต้องการ (คาดหวัง: {link_receipt})", "warning")
                                    except Exception as e:
                                        log(f"⚠️ เกิดข้อผิดพลาดในการ navigate ไปที่ Link_receipt: {str(e)[:100]}", "warning")
                                else:
                                    log("⚠️ ไม่พบ Link_receipt ใน config.py", "warning")
                                if not link_company:
                                    log("⚠️ ไม่พบ Link_conpany ใน config.py", "warning")
                            else:
                                log("⚠️ ยังอยู่ที่หน้า Login - อาจจะต้องตรวจสอบ username/password", "warning")
                            
                        except Exception as e:
                            log(f"❌ เกิดข้อผิดพลาดในการคลิกปุ่ม Login: {str(e)}", "error")
                            return False
                        
                        log("✅ กรอกข้อมูลและคลิกปุ่ม Login สำเร็จแล้ว", "success")
                        self.is_logged_in = True
                        return True
                        
                    except Exception as e:
                        log(f"❌ เกิดข้อผิดพลาด: {str(e)}", "error")
                        self.is_logged_in = False
                        return False
                
                return loop.run_until_complete(async_fill())
            
            # รัน fill ใน thread
            result = self._executor.submit(fill_async).result(timeout=60)
            return result
            
        except Exception as e:
            log(f"❌ เกิดข้อผิดพลาด: {str(e)}", "error")
            return False
    
    def login(self, username: str, password: str, link_company: Optional[str] = None, link_receipt: Optional[str] = None, log_callback: Optional[Callable] = None) -> bool:
        """
        Login เข้า PeakEngine (กรอกข้อมูล, คลิกปุ่ม Login, คลิกปุ่ม PEAK (Deprecated) และ navigate ไปที่ Link_conpany และ Link_receipt)
        
        Args:
            username (str): Username สำหรับ login
            password (str): Password สำหรับ login
            link_company (Optional[str]): URL สำหรับ navigate ไปที่ Link_conpany
            link_receipt (Optional[str]): URL สำหรับ navigate ไปที่ Link_receipt
            log_callback (Optional[Callable]): ฟังก์ชันสำหรับแสดง log (message, status)
            
        Returns:
            bool: True ถ้า login, คลิกปุ่ม PEAK (Deprecated) และ navigate สำเร็จ, False ถ้าไม่สำเร็จ
        """
        # เรียกใช้ open_login_page_and_fill() แทน
        return self.open_login_page_and_fill(username, password, link_company=link_company, link_receipt=link_receipt, log_callback=log_callback)
    
    def fill_contact_fields(
        self,
        values: List[str],
        field_selector: str = '//*[@id="iptcontactname"]',
        reg_info_map: Optional[Dict[str, Any]] = None,
        log_callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        กรอกข้อมูลลงช่องผู้ติดต่อทีละรายการตามค่าที่ได้รับ
        """
        def log(message: str, status: str = "info"):
            if log_callback:
                try:
                    log_callback(message, status)
                except:
                    pass
            logger.info(message)

        if not self.use_browser or not self.page:
            log("❌ Browser ยังไม่ได้เปิด", "error")
            return {"error": "Browser ยังไม่ได้เปิด"}

        if not self.is_logged_in:
            log("⚠️ ยังไม่ได้ Login - กรุณา Login ก่อน", "warning")
            return {"error": "ยังไม่ได้ Login"}

        def normalize_value(raw) -> str:
            if raw is None:
                return ""
            value_str = str(raw).strip()
            if not value_str:
                return ""
            digits = "".join(ch for ch in value_str if ch.isdigit())
            if not digits:
                return ""
            digits = digits[-13:]
            if len(digits) < 13:
                digits = digits.zfill(13)
            if digits[0] != "0":
                digits = "0" + digits[1:]
            return digits

        clean_values = []
        for v in values:
            normalized = normalize_value(v)
            if normalized:
                clean_values.append(normalized)

        if not clean_values:
            log("⚠️ ไม่มีค่าที่พร้อมสำหรับกรอก", "warning")
            return {"total": 0, "success": 0, "errors": []}

        def fill_async():
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            async def async_fill():
                results = {
                    "total": len(clean_values),
                    "success": 0,
                    "errors": [],
                    "processed": [],
                    "dropdown_options": [],
                    "plus_clicked": [],
                    "selected_existing": [],
                    "validation": []
                }

                for idx, value in enumerate(clean_values, 1):
                    try:
                        log(f"✏️ ({idx}/{len(clean_values)}) กำลังกรอกเลขทะเบียน: {value}", "info")
                        input_element = await self.page.wait_for_selector(field_selector, timeout=5000)
                        await input_element.click()
                        try:
                            await input_element.fill("")
                        except Exception:
                            pass
                        await asyncio.sleep(0.1)
                        await input_element.fill(value)
                        log(f"✅ กรอก {value} สำเร็จ", "success")
                        results["success"] += 1
                        results["processed"].append(value)
                        reg_info = reg_info_map.get(value) if reg_info_map else None
                        dropdown_items = []
                        selectable_option = None
                        existing_selected = False
                        selectable_text = None
                        plus_option = None
                        dropdown_container = None
                        dropdown_selectors = [
                            '//*[@id="ui-id-15"]',
                            '//*[@id="ui-id-4"]',
                            '//ul[contains(@class,"ui-autocomplete")]'
                        ]
                        for selector in dropdown_selectors:
                            try:
                                dropdown_container = await self.page.wait_for_selector(selector, timeout=1500)
                                if dropdown_container:
                                    try:
                                        is_visible = await dropdown_container.is_visible()
                                    except Exception:
                                        is_visible = True
                                    if is_visible:
                                        break
                            except Exception:
                                dropdown_container = None

                        plus_option_clicked = False
                        if dropdown_container:
                            try:
                                option_elements = await dropdown_container.query_selector_all('li')
                                for option in option_elements:
                                    try:
                                        option_text = await option.inner_text()
                                        cleaned_text = option_text.strip()
                                        if cleaned_text:
                                            dropdown_items.append(cleaned_text)
                                        if cleaned_text.startswith('+ เพิ่มผู้ติดต่อ') and selectable_option is None:
                                            plus_option = option
                                        elif selectable_option is None:
                                            selectable_option = option
                                            selectable_text = cleaned_text
                                    except Exception:
                                        continue

                                if dropdown_items:
                                    if dropdown_items == ['+ เพิ่มผู้ติดต่อ'] and plus_option is not None:
                                        try:
                                            await plus_option.click()
                                            plus_option_clicked = True
                                            log("🖱️ คลิก '+ เพิ่มผู้ติดต่อ' เพื่อเพิ่มผู้ติดต่อใหม่", "info")
                                            await asyncio.sleep(1)
                                        except Exception as click_error:
                                            log(f"⚠️ คลิก '+ เพิ่มผู้ติดต่อ' ไม่สำเร็จ: {click_error}", "warning")
                                    elif selectable_option is not None:
                                        try:
                                            await selectable_option.click()
                                            chosen_text = selectable_text or dropdown_items[min(1, len(dropdown_items)-1)]
                                            log(f"✅ เลือกรายการ '{chosen_text}' จาก dropdown", "success")
                                            await asyncio.sleep(0.5)
                                            existing_selected = True
                                            results.setdefault("selected_existing", []).append(value)
                                        except Exception as select_error:
                                            log(f"⚠️ เลือกรายการจาก dropdown ไม่สำเร็จ: {select_error}", "warning")
                            except Exception:
                                dropdown_items = []

                        if dropdown_items:
                            log(f"🧾 ตัวเลือก dropdown ({len(dropdown_items)}): {', '.join(dropdown_items[:5])}", "info")
                        else:
                            log("ℹ️ ไม่พบตัวเลือกใน dropdown หลังกรอกเลขทะเบียน", "info")

                        results["dropdown_options"].append({
                            "value": value,
                            "items": dropdown_items
                        })
                        if plus_option_clicked:
                            results["plus_clicked"].append(value)
                            try:
                                log("⏳ รอหน้าต่างเพิ่มผู้ติดต่อแสดงผล...", "info")
                                modal_field = await self.page.wait_for_selector('#mdccipttaxid1', timeout=5000)
                                if modal_field:
                                    log("✅ พบหน้าต่างเพิ่มผู้ติดต่อ - กำลังกรอกเลข 13 หลัก", "success")
                                    for idx_digit, digit in enumerate(value[:13], start=1):
                                        input_selector = f'#mdccipttaxid{idx_digit}'
                                        try:
                                            digit_input = await self.page.wait_for_selector(input_selector, timeout=1000)
                                            if digit_input:
                                                await digit_input.click()
                                                await digit_input.fill(digit)
                                                await asyncio.sleep(0.05)
                                        except Exception as digit_error:
                                            log(f"⚠️ กรอกเลขหลักที่ {idx_digit} ไม่สำเร็จ: {digit_error}", "warning")
                                    log("✅ กรอกเลข 13 หลักในหน้าต่างเพิ่มผู้ติดต่อเรียบร้อย", "success")

                                    try:
                                        search_button = await self.page.wait_for_selector('#contactgetinfobtn', timeout=2000)
                                        if search_button:
                                            log("🔍 กำลังกดปุ่ม 'ค้นหา'", "info")
                                            await search_button.click()
                                            await asyncio.sleep(0.5)
                                            for _ in range(40):
                                                status_text = ""
                                                try:
                                                    status_element = await self.page.wait_for_selector('#mdccperrmsg', timeout=200)
                                                    if status_element:
                                                        status_text = (await status_element.inner_text() or "").strip()
                                                except Exception:
                                                    status_text = ""
                                                if status_text and ("ไม่พบข้อมูลลูกค้า" in status_text or "ค้นหาสำเร็จ" in status_text):
                                                    break
                                                await asyncio.sleep(0.2)
                                            else:
                                                log("⚠️ ไม่ได้รับสถานะตอบกลับจากปุ่มค้นหาภายในเวลาที่กำหนด", "warning")
                                    except Exception as search_error:
                                        log(f"⚠️ ไม่สามารถกดปุ่มค้นหาได้: {search_error}", "warning")

                                    not_found = False
                                    success_found = False
                                    try:
                                        error_element = await self.page.wait_for_selector('#mdccperrmsg', timeout=3000)
                                        if error_element:
                                            error_text = (await error_element.inner_text() or "").strip()
                                            if "ไม่พบข้อมูลลูกค้า" in error_text:
                                                not_found = True
                                                log("ℹ️ ระบบไม่พบข้อมูลลูกค้าในฐานข้อมูล", "warning")
                                            elif "ค้นหาสำเร็จ" in error_text:
                                                success_found = True
                                                log("✅ ระบบค้นหาข้อมูลลูกค้าเรียบร้อย", "success")
                                    except Exception:
                                        pass

                                    if plus_option_clicked:
                                        if not_found and reg_info:
                                            await self._fill_contact_from_excel(value, reg_info, log)
                                            validation = await self._compare_contact_fields(reg_info, log)
                                            if validation:
                                                results.setdefault("validation", []).append(validation)
                                                if validation.get("overall_match"):
                                                    await self._confirm_create_contact(log)
                                                    await self._post_validation_tasks(reg_info, log)
                                        elif not_found and not reg_info:
                                            log("⚠️ ไม่มีข้อมูลในไฟล์ Excel สำหรับเติมในหน้าต่างเพิ่มผู้ติดต่อ", "warning")
                                        else:
                                            if success_found and reg_info:
                                                validation = await self._compare_contact_fields(reg_info, log)
                                                if validation:
                                                    results.setdefault("validation", []).append(validation)
                                                    if validation.get("overall_match"):
                                                        await self._confirm_create_contact(log)
                                                        await self._post_validation_tasks(reg_info, log)
                                            elif success_found:
                                                log("ℹ️ ระบบค้นหาสำเร็จแต่ไม่มีข้อมูล Excel สำหรับตรวจสอบ", "info")
                                    elif existing_selected and reg_info:
                                        await self._post_validation_tasks(reg_info, log)
                                else:
                                    log("⚠️ ไม่พบช่องกรอกเลข 13 หลักในหน้าต่างเพิ่มผู้ติดต่อ", "warning")
                            except Exception as modal_error:
                                log(f"⚠️ ไม่สามารถกรอกข้อมูลในหน้าต่างเพิ่มผู้ติดต่อ: {modal_error}", "warning")
                        elif existing_selected:
                            log("ℹ️ เลือกผู้ติดต่อที่มีอยู่แล้ว - ดำเนินกรอกข้อมูลต่อ", "info")
                            if reg_info:
                                await self._post_validation_tasks(reg_info, log)
                            else:
                                log("ℹ️ ไม่มีข้อมูลจาก Excel สำหรับดำเนินการต่อ", "info")
                        await asyncio.sleep(0.2)
                    except Exception as e:
                        error_msg = str(e)
                        log(f"❌ กรอก {value} ไม่สำเร็จ: {error_msg}", "error")
                        results["errors"].append({"index": idx, "value": value, "error": error_msg})
                        await asyncio.sleep(0.2)

                return results

            return loop.run_until_complete(async_fill())

        return self._executor.submit(fill_async).result(timeout=300)
    
    def execute_workflow(self, steps: List[Dict[str, Any]], log_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        Execute multi-step workflow
        
        Args:
            steps (List[Dict]): List of workflow steps, each step should have:
                - type: 'click', 'fill', 'wait', 'navigate', 'extract'
                - selector: CSS selector or text
                - value: value to fill (for 'fill' type)
                - timeout: timeout in seconds (optional)
            log_callback (Optional[Callable]): ฟังก์ชันสำหรับแสดง log
            
        Returns:
            Dict: Results from workflow execution
        """
        def log(message: str, status: str = "info"):
            """Helper function สำหรับ log"""
            if log_callback:
                try:
                    log_callback(message, status)
                except:
                    pass
            logger.info(message)
        
        if not self.use_browser or not self.page:
            log("❌ Browser ยังไม่ได้เปิด", "error")
            return {"error": "Browser ยังไม่ได้เปิด"}
        
        if not self.is_logged_in:
            log("⚠️ ยังไม่ได้ Login - กรุณา Login ก่อน", "warning")
            return {"error": "ยังไม่ได้ Login"}
        
        results = {
            "steps_completed": 0,
            "steps_total": len(steps),
            "data": [],
            "errors": []
        }
        
        try:
            def workflow_async():
                """รัน workflow operations ใน thread ด้วย async"""
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_closed():
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                except:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                async def async_workflow():
                    for i, step in enumerate(steps, 1):
                        step_type = step.get("type", "")
                        selector = step.get("selector", "")
                        value = step.get("value", "")
                        timeout = step.get("timeout", 5000)
                        
                        log(f"📋 ขั้นตอน {i}/{len(steps)}: {step_type} - {selector[:50]}...", "info")
                        
                        try:
                            if step_type == "click":
                                element = await self.page.wait_for_selector(selector, timeout=timeout)
                                await element.click()
                                log(f"✅ คลิก {selector} สำเร็จ", "success")
                                await asyncio.sleep(1)
                                
                            elif step_type == "fill":
                                element = await self.page.wait_for_selector(selector, timeout=timeout)
                                await element.fill(value)
                                log(f"✅ กรอกข้อมูล {selector} สำเร็จ", "success")
                                await asyncio.sleep(0.5)
                                
                            elif step_type == "wait":
                                wait_time = int(value) if value else 2
                                await asyncio.sleep(wait_time)
                                log(f"✅ รอ {wait_time} วินาที", "success")
                                
                            elif step_type == "navigate":
                                await self.page.goto(value, wait_until='networkidle', timeout=30000)
                                log(f"✅ ไปที่ {value} สำเร็จ", "success")
                                await asyncio.sleep(1)
                                
                            elif step_type == "extract":
                                # Extract data from current page
                                data = await self.extract_table_data_async(selector)
                                results["data"].append(data)
                                log(f"✅ ดึงข้อมูลจาก {selector} สำเร็จ", "success")
                                
                            results["steps_completed"] += 1
                            
                        except Exception as e:
                            error_msg = f"ขั้นตอน {i} ไม่สำเร็จ: {str(e)}"
                            log(error_msg, "error")
                            results["errors"].append({
                                "step": i,
                                "type": step_type,
                                "error": str(e)
                            })
                    
                    return results
                
                return loop.run_until_complete(async_workflow())
            
            # รัน workflow ใน thread
            results = self._executor.submit(workflow_async).result(timeout=300)
            return results
            
        except Exception as e:
            log(f"❌ เกิดข้อผิดพลาดในการทำงาน: {str(e)}", "error")
            results["errors"].append({"error": str(e)})
            return results
    
    async def extract_table_data_async(self, selector: str = "table") -> List[Dict]:
        """Extract table data from current page (async)"""
        try:
            tables = await self.page.query_selector_all(selector)
            all_data = []
            
            for table in tables:
                # ดึง header
                headers = []
                header_rows = await table.query_selector_all("thead tr, tr:first-child")
                if header_rows:
                    header_cells = await header_rows[0].query_selector_all("th, td")
                    headers = [await cell.inner_text() for cell in header_cells]
                
                # ดึงข้อมูล
                rows = await table.query_selector_all("tbody tr, tr:not(:first-child)")
                for row in rows:
                    cells = await row.query_selector_all("td, th")
                    row_data = {}
                    for i, cell in enumerate(cells):
                        cell_text = await cell.inner_text()
                        header = headers[i] if i < len(headers) else f"Column_{i+1}"
                        row_data[header] = cell_text.strip()
                    if row_data:
                        all_data.append(row_data)
            
            return all_data
            
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return []
    
    def extract_table_data(self, selector: str = "table") -> pd.DataFrame:
        """
        Extract table data from current page
        
        Args:
            selector (str): CSS selector for table
            
        Returns:
            pd.DataFrame: Extracted table data
        """
        if not self.use_browser or not self.page:
            return pd.DataFrame()
        
        try:
            def extract_async():
                """รัน extract operations ใน thread ด้วย async"""
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_closed():
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                except:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                async def async_extract():
                    return await self.extract_table_data_async(selector)
                
                return loop.run_until_complete(async_extract())
            
            # รัน extract ใน thread
            data = self._executor.submit(extract_async).result(timeout=30)
            
            if data:
                return pd.DataFrame(data)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error extracting table data: {str(e)}")
            return pd.DataFrame()
    
    async def _fill_contact_from_excel(self, registration_number: str, info: Dict[str, Any], log: Callable[[str, str], None]) -> None:
        try:
            dbd_info = info.get("dbd_info", {}) or {}
            row_data = info.get("row", {}) or {}
            transfer_type = info.get("transfer_type", "")
            company_name_raw = info.get("company_name", "") or dbd_info.get("ชื่อบริษัท")

            if transfer_type and "บริษัท (บจก.)" in transfer_type:
                try:
                    dropdown = await self.page.wait_for_selector('#mdccddlmerchanttype', timeout=2000)
                    if dropdown:
                        current_value = await dropdown.inner_text()
                        if "บริษัทจำกัด" not in current_value:
                            await dropdown.click()
                            await asyncio.sleep(0.2)
                            option = await self.page.wait_for_selector('#mdccddlmerchanttype .menu .item[data-value="2"]', timeout=2000)
                            if option:
                                await option.click()
                                log("✅ เลือกประเภทนิติบุคคล 'บริษัทจำกัด'", "success")
                except Exception as e:
                    log(f"⚠️ ไม่สามารถเลือกประเภทบริษัทจำกัด: {e}", "warning")

            if company_name_raw:
                cleaned_name = self._clean_company_name(company_name_raw)
                try:
                    name_input = await self.page.wait_for_selector('#contactmerchantname', timeout=2000)
                    if name_input:
                        await name_input.click()
                        await name_input.fill(cleaned_name)
                        log(f"✅ กรอกชื่อกิจการ: {cleaned_name}", "success")
                except Exception as e:
                    log(f"⚠️ ไม่สามารถกรอกชื่อกิจการ: {e}", "warning")

            if dbd_info or row_data:
                combined_address = self._format_main_address(row_data)
                address_text = combined_address or self._normalize_component(row_data.get("ที่อยู่")) or self._normalize_component(dbd_info.get("ที่อยู่"))
                if address_text:
                    try:
                        address_input = await self.page.wait_for_selector('#customerThAddress', timeout=1000)
                        if address_input:
                            await address_input.click()
                            await address_input.fill(address_text)
                            log("✅ กรอกที่อยู่จากข้อมูล DBD", "success")
                    except Exception:
                        pass

                subdistrict = self._normalize_component(row_data.get("ที่อยู่_ตำบล")) or self._normalize_component(dbd_info.get("แขวง/ตำบล"))
                if subdistrict:
                    try:
                        district1_input = await self.page.wait_for_selector('#customerThDistrict1', timeout=1000)
                        if district1_input:
                            await district1_input.click()
                            await district1_input.fill(subdistrict)
                            log("✅ กรอกแขวง/ตำบลจากข้อมูล DBD", "success")
                    except Exception:
                        pass

                district = self._normalize_component(row_data.get("ที่อยู่_อำเภอ")) or self._normalize_component(dbd_info.get("เขต/อำเภอ"))
                if district:
                    try:
                        district2_input = await self.page.wait_for_selector('#customerThDistrict2', timeout=1000)
                        if district2_input:
                            await district2_input.click()
                            await district2_input.fill(district)
                            log("✅ กรอกเขต/อำเภอจากข้อมูล DBD", "success")
                    except Exception:
                        pass

                province = self._normalize_component(row_data.get("ที่อยู่_จังหวัด")) or self._normalize_component(dbd_info.get("จังหวัด"))
                if province:
                    try:
                        province_input = await self.page.wait_for_selector('#customerThProvince', timeout=1000)
                        if province_input:
                            await province_input.click()
                            await province_input.fill(province)
                            log("✅ กรอกจังหวัดจากข้อมูล DBD", "success")
                    except Exception:
                        pass
        except Exception as e:
            log(f"⚠️ เกิดข้อผิดพลาดในการเติมข้อมูลจาก Excel: {e}", "warning")

    async def _compare_contact_fields(self, info: Dict[str, Any], log: Callable[[str, str], None]) -> Optional[Dict[str, Any]]:
        row_data = info.get("row", {})
        if not row_data:
            log("⚠️ ไม่มีข้อมูลใน Excel สำหรับใช้ตรวจสอบความถูกต้อง", "warning")
            return None

        dbd_info = info.get("dbd_info", {}) or {}

        async def get_value(selector: str) -> str:
            try:
                element = await self.page.wait_for_selector(selector, timeout=1000)
                if not element:
                    return ""
                try:
                    value = await element.input_value()
                except Exception:
                    value = await element.get_attribute("value")
                return (value or "").strip()
            except Exception:
                return ""

        def normalize(text: Any) -> str:
            if text is None or (isinstance(text, float) and pd.isna(text)):  # type: ignore
                return ""
            return re.sub(r"\s+", " ", str(text).strip()).casefold()

        comparisons = []

        expected_name = self._clean_company_name(info.get("company_name") or row_data.get("ชื่อบริษัทจาก DBD") or "")
        actual_name = await get_value("#contactmerchantname")
        comparisons.append({
            "field": "ชื่อกิจการ",
            "expected": expected_name,
            "actual": actual_name,
            "match": normalize(expected_name) == normalize(actual_name)
        })

        expected_main_address = self._format_main_address(row_data)
        actual_main_address = await get_value("#customerThAddress")
        comparisons.append({
            "field": "ที่อยู่ (บ้านเลขที่/หมู่บ้าน/หมู่ที่)",
            "expected": expected_main_address,
            "actual": actual_main_address,
            "match": normalize(expected_main_address) == normalize(actual_main_address)
        })

        expected_subdistrict = self._normalize_component(row_data.get("ที่อยู่_ตำบล")) or self._normalize_component(dbd_info.get("แขวง/ตำบล"))
        actual_subdistrict = await get_value("#customerThDistrict1")
        comparisons.append({
            "field": "แขวง/ตำบล",
            "expected": expected_subdistrict,
            "actual": actual_subdistrict,
            "match": normalize(expected_subdistrict) == normalize(actual_subdistrict)
        })

        expected_district = self._normalize_component(row_data.get("ที่อยู่_อำเภอ")) or self._normalize_component(dbd_info.get("เขต/อำเภอ"))
        actual_district = await get_value("#customerThDistrict2")
        comparisons.append({
            "field": "เขต/อำเภอ",
            "expected": expected_district,
            "actual": actual_district,
            "match": normalize(expected_district) == normalize(actual_district)
        })

        expected_province = self._normalize_component(row_data.get("ที่อยู่_จังหวัด")) or self._normalize_component(dbd_info.get("จังหวัด"))
        actual_province = await get_value("#customerThProvince")
        comparisons.append({
            "field": "จังหวัด",
            "expected": expected_province,
            "actual": actual_province,
            "match": normalize(expected_province) == normalize(actual_province)
        })

        all_match = all(item["match"] for item in comparisons)
        if all_match:
            log("✅ ข้อมูลที่ระบบค้นหากลับมาตรงกับข้อมูลใน Excel", "success")
        else:
            log("⚠️ ข้อมูลที่ระบบค้นหากลับมาไม่ตรงกับ Excel บางรายการ", "warning")

        validation_result = {
            "registration": info.get("registration"),
            "overall_match": all_match,
            "details": comparisons
        }

        return validation_result

    async def _post_validation_tasks(self, info: Dict[str, Any], log: Callable[[str, str], None]) -> None:
        row_data = info.get("row", {}) or {}
        desired_date = self._normalize_component(row_data.get("วันที่")) or self._normalize_component(info.get("date"))
        if not await self._wait_for_document_number_ready(log):
            return
        if desired_date:
            await self._fill_document_date(desired_date, log)
        else:
            log("ℹ️ ไม่มีข้อมูลวันที่จาก Excel สำหรับกรอก", "info")
        await self._fill_tarremark(row_data, log)
        await self._fill_product_template(log)

    async def _wait_for_document_number_ready(self, log: Callable[[str, str], None]) -> bool:
        try:
            await self.page.wait_for_selector('#iptnumber', timeout=5000, state='visible')
            log("✅ ช่องเอกสารเลขที่ (#iptnumber) พร้อมสำหรับกรอก", "success")
            return True
        except Exception as e:
            log(f"❌ ไม่พบช่องเอกสารเลขที่ (#iptnumber): {e}", "error")
            return False

    def _clean_company_name(self, company_name: str) -> str:
        if not company_name:
            return ""
        name = company_name.strip()
        patterns = [
            r'^\s*บริษัท\s+',
            r'\s+จำกัด\s*(\(มหาชน\))?$',
            r'^\s*บริษัทมหาชนจำกัด\s+',
            r'^\s*ห้างหุ้นส่วนจำกัด\s+'
        ]
        for pattern in patterns:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        return name.strip()
    
    async def _fill_document_date(self, desired_date: str, log: Callable[[str, str], None]) -> None:
        formatted_date = self._format_target_date(desired_date)
        if not formatted_date:
            log(f"⚠️ ไม่สามารถตีความวันที่ '{desired_date}' ได้", "warning")
            return
        try:
            date_input = await self.page.wait_for_selector('#iptdate', timeout=2000)
            if date_input:
                await date_input.click()
                await date_input.fill("")
                await date_input.fill(formatted_date)
                log(f"✅ กรอกวันที่ออกเอกสาร: {formatted_date}", "success")
            else:
                log("⚠️ ไม่พบช่องกรอกวันที่ออกเอกสาร", "warning")
        except Exception as e:
            log(f"⚠️ ไม่สามารถกรอกวันที่ออกเอกสาร: {e}", "warning")

    async def _fill_product_template(self, log: Callable[[str, str], None]) -> None:
        try:
            product_input = await self.page.wait_for_selector('#iptproducttemplateid1', timeout=2000)
            if not product_input:
                log("⚠️ ไม่พบช่องกรอกสินค้า/บริการ", "warning")
                return
            await product_input.click()
            await product_input.fill("P00001")
            await asyncio.sleep(0.3)

            product_selectors = [
                '//ul[contains(@class,"ui-autocomplete")]/li[contains(@id,"ui-id") and contains(.,"P00001")]',
                '//li[contains(@class,"ui-menu-item") and contains(.,"P00001")]'
            ]
            for selector in product_selectors:
                try:
                    option = await self.page.wait_for_selector(selector, timeout=1000)
                    if option:
                        await option.click()
                        log("✅ เลือกสินค้า/บริการ 'P00001 - ไลฟ์สดสินค้าเทศกาลเจนนี่'", "success")
                        break
                except Exception:
                    continue
            else:
                log("⚠️ ไม่พบรายการ 'P00001' ใน dropdown สินค้า/บริการ", "warning")
                return

            desired_description = "ไลฟ์สดสินค้าเทศกาลเจนนี่"
            try:
                description_area = await self.page.wait_for_selector('#iptdescription1', timeout=2000)
                if description_area:
                    desired_description = "ไลฟ์สดสินค้าเทศกาลเจนนี่"

                    async def apply_description() -> bool:
                        for attempt in range(3):
                            await description_area.click()
                            try:
                                await description_area.press("Control+A")
                            except Exception:
                                try:
                                    await description_area.press("Meta+A")
                                except Exception:
                                    pass
                            await description_area.press("Delete")
                            await description_area.fill(desired_description)
                            try:
                                await self.page.evaluate(
                                    """(value) => {
                                        const el = document.querySelector('#iptdescription1');
                                        if (el) {
                                            el.value = value;
                                            el.dispatchEvent(new Event('input', { bubbles: true }));
                                            el.dispatchEvent(new Event('change', { bubbles: true }));
                                        }
                                    }""",
                                    desired_description
                                )
                            except Exception:
                                pass

                            current_value = ""
                            try:
                                current_value = await description_area.input_value()
                            except Exception:
                                current_value = await self.page.evaluate(
                                    """() => {
                                        const el = document.querySelector('#iptdescription1');
                                        return el ? el.value || '' : '';
                                    }"""
                                )

                            if current_value.strip() == desired_description:
                                log("✅ ปรับรายละเอียดสินค้าเป็น 'ไลฟ์สดสินค้าเทศกาลเจนนี่'", "success")
                                return True

                            if attempt < 2:
                                log("⚠️ รายละเอียดสินค้ายังไม่ตรง รอ 0.5 วินาทีแล้วลองอีกครั้ง", "warning")
                                await asyncio.sleep(0.5)

                        log("❌ ไม่สามารถตั้งรายละเอียดสินค้าให้ตรงกับค่าที่ต้องการได้", "error")
                        return False

                    first_apply_success = await apply_description()

                    if first_apply_success:
                        try:
                            amount_field = await self.page.wait_for_selector('#iptamount1', timeout=2000)
                            if amount_field:
                                await amount_field.click()
                                await asyncio.sleep(0.1)
                                log("✅ คลิกช่องจำนวน (#iptamount1) เพื่อแก้ไขค่า", "success")
                            else:
                                log("⚠️ ไม่พบช่องจำนวน (#iptamount1)", "warning")
                        except Exception as amount_error:
                            log(f"⚠️ ไม่สามารถคลิกช่องจำนวน (#iptamount1): {amount_error}", "warning")

                        await apply_description()
                else:
                    log("⚠️ ไม่พบช่องรายละเอียดสินค้า (#iptdescription1)", "warning")
            except Exception as desc_error:
                log(f"⚠️ ไม่สามารถปรับรายละเอียดสินค้า: {desc_error}", "warning")
        except Exception as e:
            log(f"⚠️ ไม่สามารถเลือกสินค้า/บริการ: {e}", "warning")

    async def _fill_tarremark(self, row_data: Dict[str, Any], log: Callable[[str, str], None]) -> None:
        description_text = self._normalize_component(row_data.get("คำอธิบาย"))
        account_suffix = self._extract_account_suffix(description_text)
        if not account_suffix:
            log("ℹ️ ไม่พบเลขบัญชี 4 หลักสำหรับใส่ในหมายเหตุ", "info")
        date_text = self._normalize_component(row_data.get("วันที่"))
        time_text = self._normalize_component(row_data.get("เวลา"))
        if not date_text and not time_text:
            combined = (
                self._normalize_component(row_data.get("วันที่ เวลา X5711"))
                or self._normalize_component(row_data.get("วันที่ เวลา"))
                or ""
            )
            if combined:
                parts = combined.split()
                if parts:
                    date_text = parts[0]
                if len(parts) > 1:
                    time_text = parts[1]

        if date_text:
            formatted_date = self._format_target_date(date_text)
            if formatted_date:
                date_text = formatted_date

        date_time_text = " ".join(filter(None, [date_text, time_text])).strip()

        if not date_time_text and not account_suffix:
            log("ℹ️ ไม่มีข้อมูลสำหรับกรอกอ้างอิง (#iptrefname)", "info")
            return
        remark_value = date_time_text.strip()
        if account_suffix:
            remark_value = f"{remark_value} {account_suffix}".strip()
        try:
            remark_field = await self.page.wait_for_selector('#iptrefname', timeout=2000)
            if not remark_field:
                log("⚠️ ไม่พบช่องอ้างอิง (#iptrefname)", "warning")
                return
            await remark_field.click()
            try:
                await remark_field.press("Control+A")
            except Exception:
                try:
                    await remark_field.press("Meta+A")
                except Exception:
                    pass
            await remark_field.press("Delete")
            await remark_field.fill(remark_value)
            try:
                await self.page.evaluate(
                    """(value) => {
                        const el = document.querySelector('#iptrefname');
                        if (el) {
                            el.value = value;
                            el.dispatchEvent(new Event('input', { bubbles: true }));
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                        }
                    }""",
                    remark_value
                )
            except Exception:
                pass
            log(f"✅ กรอกอ้างอิงเป็น '{remark_value}'", "success")
        except Exception as e:
            log(f"⚠️ ไม่สามารถกรอกอ้างอิง: {e}", "warning")

    def _format_target_date(self, value: str) -> Optional[str]:
        if not value:
            return None
        text = value.strip()
        fmt_list = [
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%d/%m/%y",
            "%Y-%m-%d",
            "%d %b %Y",
            "%d %b %y"
        ]
        for fmt in fmt_list:
            try:
                dt = datetime.strptime(text, fmt)
                return dt.strftime("%d/%m/%Y")
            except Exception:
                continue
        thai_pattern = re.match(r"(\d{1,2})\s+([ก-๙]+)\s+(\d{4})", text)
        if thai_pattern:
            day = int(thai_pattern.group(1))
            month_name = thai_pattern.group(2)
            year = int(thai_pattern.group(3))
            thai_months = {
                "ม.ค.": 1, "มกราคม": 1,
                "ก.พ.": 2, "กุมภาพันธ์": 2,
                "มี.ค.": 3, "มีนาคม": 3,
                "เม.ย.": 4, "เมษายน": 4,
                "พ.ค.": 5, "พฤษภาคม": 5,
                "มิ.ย.": 6, "มิถุนายน": 6,
                "ก.ค.": 7, "กรกฎาคม": 7,
                "ส.ค.": 8, "สิงหาคม": 8,
                "ก.ย.": 9, "กันยายน": 9,
                "ต.ค.": 10, "ตุลาคม": 10,
                "พ.ย.": 11, "พฤศจิกายน": 11,
                "ธ.ค.": 12, "ธันวาคม": 12
            }
            month = thai_months.get(month_name, 0)
            if not month:
                return None
            if year > 2500:
                year -= 543
            try:
                dt = datetime(year, month, day)
                return dt.strftime("%d/%m/%Y")
            except Exception:
                return None
        return None
    
    async def _confirm_create_contact(self, log: Callable[[str, str], None]) -> None:
        try:
            create_button = await self.page.wait_for_selector('#contactcreatebtn', timeout=2000)
            if create_button:
                log("📝 กำลังกดปุ่ม 'เพิ่มลูกค้า/ผู้จ่ายเงิน'", "info")
                await create_button.click()
                await asyncio.sleep(0.5)
        except Exception as e:
            log(f"⚠️ ไม่สามารถกดปุ่มเพิ่มลูกค้า/ผู้จ่ายเงินได้: {e}", "warning")
    
    def _normalize_component(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            if pd.isna(value):
                return ""
            if value.is_integer():
                value = int(value)
        text = str(value).strip()
        return "" if text in ("", "0", "-", "--") else text

    def _extract_account_suffix(self, description: str) -> Optional[str]:
        if not description:
            return None
        match = re.search(r'X(\d{4})', description)
        if match:
            return f"X{match.group(1)}"
        return None

    def _format_main_address(self, row_data: Dict[str, Any]) -> str:
        house = self._normalize_component(row_data.get("ที่อยู่_บ้านเลขที่"))
        village = self._normalize_component(row_data.get("ที่อยู่_หมู่บ้าน"))
        moo = self._normalize_component(row_data.get("ที่อยู่_หมู่ที่"))
        parts = []
        if house:
            parts.append(f"เลขที่ {house}")
        if village:
            parts.append(f"หมู่บ้าน {village}")
        if moo:
            parts.append(f"หมู่ {moo}")
        return " ".join(parts).strip()
    
    def close(self):
        """Close browser"""
        if self.browser:
            try:
                def close_async():
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_closed():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                    except:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    
                    async def async_close():
                        await self.browser.close()
                        await self.playwright.stop()
                    
                    loop.run_until_complete(async_close())
                
                self._executor.submit(close_async).result(timeout=10)
                logger.info("✅ ปิด Browser สำเร็จ")
            except Exception as e:
                logger.error(f"Error closing browser: {str(e)}")
        
        if self._executor:
            self._executor.shutdown(wait=True)




