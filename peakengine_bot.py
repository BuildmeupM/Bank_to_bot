import pandas as pd
import time
import re
from typing import Dict, List, Optional, Callable, Any
import logging
from concurrent.futures import ThreadPoolExecutor
import asyncio

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
                        return True
                        
                    except Exception as e:
                        log(f"❌ เกิดข้อผิดพลาด: {str(e)}", "error")
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




