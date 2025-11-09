import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from typing import Dict, Optional, Callable
import logging
from concurrent.futures import ThreadPoolExecutor
import asyncio

def parse_thai_address(address: str) -> Dict[str, str]:
    """แยกองค์ประกอบที่อยู่ภาษาไทยออกเป็นส่วนๆ"""
    components = {
        "house_no": "",
        "village": "",
        "moo": "",
        "subdistrict": "",
        "district": "",
        "province": "",
        "postal_code": ""
    }

    if not address:
        return components

    text = str(address)
    text = re.sub(r"\s+", " ", text).strip().strip(',')

    if not text:
        return components

    def remove_match(src: str, match: re.Match) -> str:
        return (src[:match.start()] + src[match.end():]).strip(' ,')

    postal_match = re.search(r"(\d{5})(?!.*\d)", text)
    if postal_match:
        components["postal_code"] = postal_match.group(1)
        text = remove_match(text, postal_match)

    province_patterns = [
        r"(?:จังหวัด|จ\.)\s*([ก-๙]+)",
        r"(กรุงเทพมหานคร)"
    ]
    for pattern in province_patterns:
        province_match = re.search(pattern, text)
        if province_match:
            group_index = province_match.lastindex or 0
            components["province"] = province_match.group(group_index).strip()
            text = remove_match(text, province_match)
            break

    district_match = re.search(r"(?:อำเภอ|อ\.|เขต)\s*([ก-๙]+(?:\s[ก-๙]+)*)", text)
    if district_match:
        components["district"] = district_match.group(1).strip()
        text = remove_match(text, district_match)

    subdistrict_match = re.search(r"(?:ตำบล|ต\.|แขวง)\s*([ก-๙]+(?:\s[ก-๙]+)*)", text)
    if subdistrict_match:
        components["subdistrict"] = subdistrict_match.group(1).strip()
        text = remove_match(text, subdistrict_match)

    moo_match = re.search(r"(?:หมู่ที่|หมู่)\s*([\d]+)", text)
    if moo_match:
        components["moo"] = moo_match.group(1).strip()
        text = remove_match(text, moo_match)

    village_match = re.search(r"(?:หมู่บ้าน|บ้าน)\s*([ก-๙0-9\s]+?)(?=(?:หมู่ที่|หมู่|ตำบล|ต\.|แขวง|อำเภอ|อ\.|เขต|จังหวัด|จ\.|กรุงเทพ|$))", text)
    if village_match:
        components["village"] = village_match.group(1).strip()
        text = remove_match(text, village_match)

    house_match = re.search(r"^(?:บ้านเลขที่|เลขที่)?\s*([^\s,]+)", text)
    if house_match:
        components["house_no"] = house_match.group(1).strip()
        text = remove_match(text, house_match)

    if not components["house_no"]:
        fallback_match = re.search(r"([0-9]+[\/0-9-]*)", text)
        if fallback_match and fallback_match.start() == 0:
            components["house_no"] = fallback_match.group(1).strip()
            text = remove_match(text, fallback_match)

    if not components["province"] and "กรุงเทพมหานคร" in address:
        components["province"] = "กรุงเทพมหานคร"

    return components

# ตั้งค่า logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DBDDataWarehouseBot:
    """คลาสสำหรับดึงข้อมูลจาก DBD DataWarehouse"""
    
    def __init__(self, use_browser: bool = False, headless: bool = False):
        """
        Initialize bot
        
        Args:
            use_browser (bool): ใช้ browser (Playwright) แทน requests
            headless (bool): เปิด browser แบบ headless (ซ่อนหน้าจอ)
        """
        self.base_url = "https://datawarehouse.dbd.go.th"
        self.search_url = f"{self.base_url}/index"
        self.use_browser = use_browser
        self.headless = False  # บังคับให้แสดง browser เสมอ เพื่อให้เห็นการทำงาน
        self.browser = None
        self.page = None
        self.playwright = None
        self._executor = None
        
        if use_browser:
            try:
                from playwright.async_api import async_playwright
                
                # ใช้ ThreadPoolExecutor เพื่อหลีกเลี่ยงปัญหา event loop ใน Streamlit
                self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="playwright")
                
                def init_playwright_in_new_event_loop():
                    """สร้าง event loop ใหม่ใน thread เพื่อหลีกเลี่ยงปัญหา Streamlit"""
                    # สร้าง event loop ใหม่ใน thread นี้
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        async def async_init():
                            logger.info("🚀 กำลังเปิด Playwright Browser (async mode)...")
                            pw = await async_playwright().start()
                            
                            logger.info("🌐 กำลัง launch Chromium browser...")
                            browser = await pw.chromium.launch(
                                headless=False,  # แสดงหน้าจอ browser เสมอ
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
                        
                        # รัน async function ใน event loop ใหม่
                        return loop.run_until_complete(async_init())
                    finally:
                        # เก็บ event loop ไว้ใช้งานต่อไป
                        pass
                
                logger.info("🚀 กำลังเริ่มต้น Playwright Browser ใน thread แยก...")
                logger.info("👀 Browser จะเปิดขึ้นมาในอีกสักครู่...")
                
                # รันใน thread แยก
                future = self._executor.submit(init_playwright_in_new_event_loop)
                self.playwright, self.browser, self.page = future.result(timeout=60)
                
                # เก็บ event loop ไว้
                logger.info("✅ เปิด Playwright Browser (แสดงหน้าจอ) สำเร็จ!")
                logger.info("👀 Browser window กำลังเปิดขึ้นมา - ดูได้เลย!")
                logger.info("🌐 Browser จะปรากฏหน้าต่างใหม่ - ดูการทำงานแบบเรียลไทม์ได้เลย!")
                    
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ ไม่สามารถเปิด Playwright Browser ได้: {error_msg}")
                logger.error(f"⚠️ Error details: {type(e).__name__}: {error_msg}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                raise
                
            except ImportError as ie:
                error_msg = "playwright ไม่ได้ติดตั้ง"
                logger.error(f"❌ {error_msg}")
                logger.info("💡 ติดตั้งด้วย: pip install playwright && playwright install chromium")
                raise Exception(f"{error_msg} - โปรดติดตั้ง Playwright ก่อนใช้งาน Browser Mode")
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ ไม่สามารถเปิด Browser ได้: {error_msg}")
                # ไม่ fallback ไปใช้ requests - แสดง error ให้ผู้ใช้เห็น
                raise Exception(f"ไม่สามารถเปิด Browser ได้: {error_msg}\n\n💡 ตรวจสอบ:\n1. Playwright ติดตั้งแล้ว: pip install playwright\n2. Browser binaries ติดตั้งแล้ว: playwright install chromium\n3. ไม่มีปัญหากับ Windows event loop")
        else:
            self._init_requests_session()
    
    def _init_requests_session(self):
        """Initialize requests session with proper headers"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
    def _add_address_components(self, company_info: Dict) -> Dict:
        """เพิ่มข้อมูลที่อยู่แยกส่วนลงใน company_info"""
        if not isinstance(company_info, dict):
            return company_info

        address_text = company_info.get("address", "")
        components = parse_thai_address(address_text)

        company_info["address_components"] = components
        company_info["address_house_no"] = components.get("house_no", "")
        company_info["address_village"] = components.get("village", "")
        company_info["address_moo"] = components.get("moo", "")
        company_info["address_subdistrict"] = components.get("subdistrict", "")
        company_info["address_district"] = components.get("district", "")
        company_info["address_province"] = components.get("province", "")
        company_info["address_postal_code"] = components.get("postal_code", "")

        return company_info

    def _normalize_directors_data(self, company_info: Dict) -> Dict:
        """จัดรูปแบบข้อมูลรายชื่อกรรมการให้สะอาดและพร้อมใช้งาน"""
        if not isinstance(company_info, dict):
            return company_info

        raw_text = company_info.get("directors", "") or company_info.get("directors_raw", "")

        if not raw_text:
            company_info["directors_raw"] = ""
            company_info["directors_list"] = []
            company_info["directors"] = ""
            return company_info

        text = str(raw_text).replace('\r', '\n')
        lines = [line.strip() for line in text.split('\n') if line.strip()]

        normalized = []
        for line in lines:
            if 'รายชื่อกรรมการ' in line or 'กรรมการ:' in line:
                continue

            cleaned = re.sub(r'^[\u2022\u2023\u25E6\u2043\u2219\-\–\—\•\·\▪\▫\»]*', '', line).strip()
            cleaned = re.sub(r'^\d+[\.)]?\s*', '', cleaned)
            cleaned = cleaned.strip()

            if cleaned:
                normalized.append(cleaned)

        unique_directors = []
        for name in normalized:
            if name not in unique_directors:
                unique_directors.append(name)

        company_info["directors_raw"] = text.strip()
        company_info["directors_list"] = unique_directors
        company_info["directors"] = " | ".join(unique_directors)

        return company_info

    def _post_process_company_info(self, company_info: Dict) -> Dict:
        """จัดการข้อมูลเพิ่มเติมหลังดึงเสร็จ"""
        if not isinstance(company_info, dict):
            return company_info

        company_info = self._add_address_components(company_info)
        company_info = self._normalize_directors_data(company_info)
        return company_info

    def search_company_info(self, company_name: str, log_callback: Optional[Callable] = None) -> Dict:
        """
        ค้นหาข้อมูลบริษัทจาก DBD DataWarehouse
        
        Args:
            company_name (str): ชื่อบริษัทที่ต้องการค้นหา
            log_callback (Optional[Callable]): ฟังก์ชันสำหรับแสดง log (message, status)
            
        Returns:
            Dict: ข้อมูลบริษัทที่พบ
        """
        def log(message: str, status: str = "info"):
            """Helper function สำหรับ log"""
            if log_callback:
                try:
                    log_callback(message, status)
                except:
                    pass
            logger.info(message)
        
        try:
            # ทำความสะอาดชื่อบริษัท
            clean_name = self.clean_company_name(company_name)
            log(f"กำลังค้นหาข้อมูล: {clean_name}")
            
            if not clean_name:
                log("ไม่พบชื่อบริษัทที่ถูกต้อง", "error")
                return {"error": "ไม่พบชื่อบริษัทที่ถูกต้อง"}
            
            if self.use_browser and self.page:
                # ใช้ Playwright browser - รันใน thread ด้วย async
                log("ใช้ Playwright Browser Mode ในการค้นหา", "info")
                
                def search_with_playwright_async():
                    """รัน Playwright operations ใน thread ด้วย async"""
                    # ใช้ event loop ที่มีอยู่หรือสร้างใหม่
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_closed():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                    except:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    
                    async def async_search():
                        try:
                            log("🌐 กำลังเปิด Chromium Browser...", "info")
                            log("👀 Browser จะปรากฏขึ้นมาในอีกสักครู่ - ดูการทำงานแบบเรียลไทม์ได้เลย!", "success")
                            await asyncio.sleep(0.5)  # ให้เวลา browser เปิดก่อน
                            
                            log("📍 กำลังเข้าหน้าเว็บ DBD DataWarehouse...", "info")
                            log(f"🔗 URL: {self.search_url}", "info")
                            await self.page.goto(self.search_url, wait_until='networkidle', timeout=30000)
                            
                            # รอให้หน้าเว็บโหลดเสร็จ
                            await self.page.wait_for_load_state('domcontentloaded')
                            log("✅ โหลดหน้าเว็บเสร็จแล้ว - ดูใน Browser window ได้เลย!", "success")
                            await asyncio.sleep(0.8)
                            
                            # ปิด warning modal หากมีแสดงขึ้นมา
                            try:
                                if await self.page.is_visible('#warningModal'):
                                    log("⚠️ พบหน้าต่างแจ้งเตือน (warningModal) กำลังปิด...", "warning")
                                    # หาและคลิกปุ่มปิด
                                    close_selectors = [
                                        '#btnWarning',
                                        '#warningModal button.btn',
                                        '#warningModal button',
                                        'button:has-text("ปิด")'
                                    ]
                                    close_button = None
                                    for selector in close_selectors:
                                        try:
                                            close_button = await self.page.query_selector(selector)
                                            if close_button:
                                                break
                                        except:
                                            continue
                                    if close_button:
                                        await close_button.click()
                                        await asyncio.sleep(1)
                                        log("✅ ปิดหน้าต่างแจ้งเตือนสำเร็จ", "success")
                                    else:
                                        log("⚠️ ไม่พบปุ่มปิด warningModal", "warning")
                            except Exception as modal_error:
                                log(f"⚠️ ปิด warningModal ไม่สำเร็จ: {modal_error}", "warning")

                            log("🔍 กำลังค้นหาช่องกรอกข้อมูลในหน้าเว็บ...", "info")
                            log("👀 ดู Browser window - จะเห็นการสแกนหา input field", "info")
                            # หาช่องค้นหา - ลองหลายวิธี
                            search_input = None
                            selectors = [
                                '#key-word',
                                'input[name="search_value"]',
                                'input[type="text"]',
                                'input#search_value',
                                'input.search-input',
                                'input.form-control'
                            ]
                            
                            for selector in selectors:
                                try:
                                    search_input = await self.page.wait_for_selector(selector, timeout=5000)
                                    if search_input:
                                        log(f"✅ พบช่องค้นหาด้วย selector: {selector}", "success")
                                        log("👀 ดู Browser window - จะเห็นการ highlight ช่องค้นหา", "info")
                                        break
                                except:
                                    continue
                            
                            if not search_input:
                                # ลองหา input แรกที่เจอ
                                try:
                                    search_input = await self.page.query_selector('input')
                                except:
                                    pass
                            
                            if search_input:
                                log("⌨️ กำลังกรอกชื่อบริษัท: " + clean_name, "info")
                                log("👀 ดู Browser window - จะเห็นการพิมพ์ข้อความ", "info")
                                await search_input.fill('')  # ล้างข้อมูลเก่า
                                await search_input.fill(clean_name)
                                await asyncio.sleep(1)  # เพิ่มเวลาให้เห็นการพิมพ์
                                
                                log("🔘 กำลังกดปุ่มค้นหา...", "info")
                                log("👀 ดู Browser window - จะเห็นการคลิกปุ่มค้นหา", "info")
                                
                                # พยายามค้นหาโดยเริ่มจาก searchicon โดยตรง
                                search_button = None
                                direct_button = await self.page.query_selector('#searchicon')
                                if direct_button:
                                    search_button = direct_button
                                    log("✅ พบปุ่มค้นหา #searchicon", "success")
                                else:
                                    # ลองหา selector อื่นๆ
                                    button_selectors = [
                                        'button[type="submit"]',
                                        'input[type="submit"]',
                                        'button:has-text("ค้นหา")',
                                        'button:has-text("Search")',
                                        '.btn-search',
                                        '.search-btn'
                                    ]
                                    for selector in button_selectors:
                                        try:
                                            search_button = await self.page.query_selector(selector)
                                            if search_button:
                                                log(f"✅ พบปุ่มค้นหาด้วย selector: {selector}", "success")
                                                break
                                        except:
                                            continue
                                
                                if search_button:
                                    log("🔘 กำลังกดปุ่มค้นหา (ผ่านปุ่มค้นหา)", "info")
                                    try:
                                        await search_button.click()
                                        await asyncio.sleep(0.4)
                                        log("✅ กดปุ่มค้นหาสำเร็จ", "success")
                                    except Exception as click_error:
                                        log(f"⚠️ คลิกปุ่ม searchicon ไม่สำเร็จ: {click_error} -> ลองกด Enter", "warning")
                                        try:
                                            await search_input.press('Enter', timeout=5000)
                                        except:
                                            log("⚠️ กด Enter ไม่สำเร็จ", "warning")
                                else:
                                    log("⚠️ ไม่พบปุ่มค้นหา -> กด Enter แทน", "warning")
                                    try:
                                        await search_input.press('Enter', timeout=5000)
                                        log("✅ กด Enter สำเร็จ", "success")
                                    except Exception as enter_error:
                                        log(f"⚠️ กด Enter ไม่สำเร็จ: {enter_error}", "warning")
                                
                                log("⏳ กำลังรอผลลัพธ์จากเว็บ...", "info")
                                log("👀 ดู Browser window - กำลังโหลดผลลัพธ์", "info")
                                # รอผลลัพธ์
                                await self.page.wait_for_load_state('networkidle', timeout=15000)
                                await asyncio.sleep(1.0)
                                
                                log("📊 กำลังอ่านข้อมูลผลลัพธ์...", "info")
                                log("👀 ดู Browser window - จะเห็นผลลัพธ์ในหน้าเว็บ", "info")
                                
                                # ดึงข้อมูลจาก xpath โดยตรงด้วย Playwright
                                company_info = await self.extract_company_data_from_page(clean_name)
                                company_info = self._post_process_company_info(company_info)
                                
                                if company_info.get("registration_number"):
                                    log(f"พบข้อมูลบริษัท: {company_info.get('registration_number')}", "success")
                                else:
                                    log("ไม่พบข้อมูลบริษัท", "warning")
                                
                                return company_info
                            else:
                                log("ไม่พบช่องกรอกข้อมูลในการค้นหา", "error")
                                return {"error": "ไม่พบช่องกรอกข้อมูลในการค้นหา"}
                        except Exception as e:
                            log(f"เกิดข้อผิดพลาดใน Browser Mode: {str(e)}", "error")
                            logger.error(f"Playwright error: {str(e)}", exc_info=True)
                            return {"error": f"เกิดข้อผิดพลาด: {str(e)}"}
                    
                    return loop.run_until_complete(async_search())
                
                # รัน Playwright operations ใน thread
                try:
                    if self._executor:
                        future = self._executor.submit(search_with_playwright_async)
                        result = future.result(timeout=90)
                    else:
                        result = search_with_playwright_async()
                    return self._post_process_company_info(result)
                except Exception as e:
                    log(f"เกิดข้อผิดพลาดในการรัน Playwright: {str(e)}", "error")
                    return {"error": f"เกิดข้อผิดพลาด: {str(e)}"}
            else:
                # ใช้ requests - ต้องใช้ browser เพราะเว็บอาจมี JavaScript protection
                log("ใช้ Requests Mode ในการค้นหา", "info")
                log("⚠️ หมายเหตุ: Requests อาจไม่ทำงานเนื่องจากเว็บมี JavaScript protection", "warning")
                log("💡 แนะนำให้ใช้ Browser Mode แทน", "info")
                
                # ลอง GET request ก่อน
                try:
                    log("กำลังเข้าถึงหน้าค้นหา...", "info")
                    response = self.session.get(self.search_url, timeout=10)
                    
                    if response.status_code == 200:
                        log("ได้รับหน้าค้นหาแล้ว กำลังค้นหา...", "info")
                        # ดูว่าเว็บต้องการอะไร - อาจต้องใช้ form หรือ JavaScript
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # ลองหา search form
                        form = soup.find('form')
                        if form and form.get('action'):
                            search_url = form.get('action')
                            if not search_url.startswith('http'):
                                search_url = self.base_url + search_url
                            
                            # ส่ง POST ไปที่ form action
                            log(f"พบ form action: {search_url}", "info")
                            response = self.session.post(
                                search_url,
                                data={'search_value': clean_name},
                                timeout=10
                            )
                        else:
                            # ลองใช้ query parameter
                            log("ลองใช้ GET with query parameters...", "info")
                            response = self.session.get(
                                self.search_url,
                                params={'search_value': clean_name, 'search_type': 'company_name'},
                                timeout=10
                            )
                        
                        if response.status_code == 200:
                            log("ได้รับข้อมูลสำเร็จ", "success")
                            soup = BeautifulSoup(response.content, 'html.parser')
                            company_info = self.parse_company_data(soup, clean_name)
                            company_info = self._post_process_company_info(company_info)
                            
                            if company_info.get("registration_number"):
                                log(f"พบข้อมูลบริษัท: {company_info.get('registration_number')}", "success")
                            else:
                                log("ไม่พบข้อมูลบริษัท", "warning")
                            
                            return company_info
                        else:
                            log(f"ไม่สามารถค้นหาได้ (Status: {response.status_code})", "error")
                            return {"error": f"ไม่สามารถค้นหาข้อมูลได้ (Status: {response.status_code}) - แนะนำให้ใช้ Browser Mode"}
                    else:
                        log(f"ไม่สามารถเข้าถึงเว็บได้ (Status: {response.status_code})", "error")
                        return {"error": f"ไม่สามารถเข้าถึง DBD DataWarehouse ได้ (Status: {response.status_code}) - แนะนำให้ใช้ Browser Mode"}
                except Exception as e:
                    log(f"เกิดข้อผิดพลาดในการใช้ Requests: {str(e)}", "error")
                    return {"error": f"เกิดข้อผิดพลาด: {str(e)} - แนะนำให้ใช้ Browser Mode"}
                
        except Exception as e:
            error_msg = f"เกิดข้อผิดพลาด: {str(e)}"
            log(error_msg, "error")
            logger.error(f"เกิดข้อผิดพลาดในการค้นหาข้อมูลบริษัท {company_name}: {str(e)}")
            return {"error": error_msg}
    
    def __del__(self):
        """Close browser when object is deleted"""
        try:
            if self.page:
                try:
                    def close_page():
                        try:
                            self.page.close()
                        except:
                            pass
                    if self._executor:
                        self._executor.submit(close_page).result(timeout=5)
                    else:
                        close_page()
                except:
                    pass
            if self.browser:
                try:
                    def close_browser():
                        try:
                            self.browser.close()
                        except:
                            pass
                    if self._executor:
                        self._executor.submit(close_browser).result(timeout=5)
                    else:
                        close_browser()
                except:
                    pass
            if self.playwright:
                try:
                    def stop_playwright():
                        try:
                            self.playwright.stop()
                        except:
                            pass
                    if self._executor:
                        self._executor.submit(stop_playwright).result(timeout=5)
                        self._executor.shutdown(wait=False)
                    else:
                        stop_playwright()
                except:
                    pass
            if self._executor:
                try:
                    self._executor.shutdown(wait=False)
                except:
                    pass
        except:
            pass
    
    def clean_company_name(self, company_name: str) -> str:
        """
        ทำความสะอาดชื่อบริษัท
        
        Args:
            company_name (str): ชื่อบริษัทดิบ
            
        Returns:
            str: ชื่อบริษัทที่ทำความสะอาดแล้ว
        """
        if not company_name or pd.isna(company_name):
            return ""
        
        # ลบคำที่ไม่ต้องการ
        clean_name = str(company_name).strip()
        clean_name = re.sub(r'\b(บริษัท|บจก\.?|จำกัด|มหาชน|ห้างหุ้นส่วน|หจก\.?)\b', '', clean_name, flags=re.IGNORECASE)
        clean_name = re.sub(r'\+\+', '', clean_name)  # ลบ ++
        clean_name = re.sub(r'\s+', ' ', clean_name)  # ลบช่องว่างซ้ำ
        clean_name = clean_name.strip()
        
        return clean_name
    
    def _parse_card_info_text(self, raw_text: str, label_map: Dict[str, str]) -> Dict[str, str]:
        """แยกข้อมูลจากข้อความภายในการ์ด (card-infos) ตาม label ที่กำหนด"""
        results = {value: "" for value in label_map.values()}

        if not raw_text:
            return results

        lines = [line.strip() for line in raw_text.split('\n') if line.strip()]
        current_key = None

        for line in lines:
            normalized = line.replace(':', '').strip()
            matched_label = None

            for label_text, key_name in label_map.items():
                if normalized == label_text:
                    matched_label = key_name
                    break

            if matched_label:
                current_key = matched_label
                value = ""
                if ':' in line:
                    value = line.split(':', 1)[1].strip()

                if value:
                    results[current_key] = value
                    current_key = None
                else:
                    results[current_key] = ""
                continue

            if current_key:
                if results[current_key]:
                    results[current_key] += f" {line}"
                else:
                    results[current_key] = line

        return results

    async def extract_company_data_from_page(self, company_name: str) -> Dict:
        """
        ดึงข้อมูลบริษัทจากหน้าเว็บโดยใช้ XPath
        """
        try:
            company_info = {
                "company_name": company_name,
                "registration_number": "",
                "business_type": "",
                "status": "",
                "registered_capital": "",
                "address": "",
                "phone": "",
                "email": "",
                "found_date": "",
                "last_update": "",
                "directors": "",
                "authorized_signatories": "",
                "business_type_registration": "",
                "business_type_registration_objective": "",
                "business_type_registration_raw": "",
                "business_type_latest": "",
                "business_type_latest_objective": "",
                "business_type_latest_raw": "",
                "directors_list": []
            }
            
            # 1. ดึงชื่อนิติบุคคลและเลขทะเบียนนิติบุคคล จาก xpath: //*[@id="companyProfileTab1"]/div[1]/div[1]/div
            try:
                name_reg_element = self.page.locator('//*[@id="companyProfileTab1"]/div[1]/div[1]/div').first
                if await name_reg_element.is_visible(timeout=3000):
                    name_reg_text = await name_reg_element.inner_text()
                    
                    # แยกชื่อและเลขทะเบียน
                    lines = name_reg_text.strip().split('\n')
                    for line in lines:
                        line = line.strip()
                        if 'ชื่อนิติบุคคล' in line:
                            if ':' in line:
                                company_info["company_name"] = line.split(':', 1)[1].strip()
                            elif not company_info["company_name"]:
                                company_info["company_name"] = line.replace('ชื่อนิติบุคคล', '').strip()
                        elif 'เลขทะเบียนนิติบุคคล' in line:
                            if ':' in line:
                                company_info["registration_number"] = line.split(':', 1)[1].strip()
                            elif not company_info["registration_number"]:
                                company_info["registration_number"] = line.replace('เลขทะเบียนนิติบุคคล', '').strip()
                    
                    logger.info(f"✅ ดึงชื่อและเลขทะเบียน: ชื่อ={company_info.get('company_name', 'N/A')}, เลขทะเบียน={company_info.get('registration_number', 'N/A')}")
            except Exception as e:
                logger.warning(f"⚠️ ไม่สามารถดึงชื่อ/เลขทะเบียนได้: {str(e)}")
            
            # 2. ดึงข้อมูลนิติบุคคลทั้งหมด: //*[@id="companyProfileTab1"]/div[2]/div[1]/div[1]/div
            try:
                info_element = self.page.locator('//*[@id="companyProfileTab1"]/div[2]/div[1]/div[1]/div').first
                if await info_element.is_visible(timeout=3000):
                    info_text = await info_element.inner_text()
                    company_info["company_details"] = info_text
                    
                    # แยกข้อมูลสำคัญ - แยกรายละเอียดทั้งหมด
                    current_section = None
                    label_map = {
                        "ประเภทนิติบุคคล": "business_type",
                        "สถานะนิติบุคคล": "status",
                        "วันที่จดทะเบียนจัดตั้ง": "found_date",
                        "ทุนจดทะเบียน": "registered_capital",
                        "เลขทะเบียนเดิม": "old_registration_number",
                        "กลุ่มธุรกิจ": "business_group",
                        "ขนาดธุรกิจ": "business_size",
                        "ปีที่ส่งงบการเงิน": "financial_years",
                        "ที่ตั้งสำนักงานแห่งใหญ่": "address",
                        "Website": "website"
                    }

                    pending_label = None

                    for line in info_text.split('\n'):
                        line = line.strip()
                        if not line:
                            continue
                        
                        # ตรวจจับหัวข้อหลัก (ไม่มี ":")
                        if 'ข้อมูลนิติบุคคล' in line and ':' not in line:
                            current_section = "company_info"
                            continue
                        elif 'กลุ่มธุรกิจ' in line and ':' not in line:
                            current_section = "business_group"
                            continue
                        elif 'ปีที่ส่งงบการเงิน' in line and ':' not in line:
                            current_section = "financial_years"
                            if '(คลิกที่ปีเพื่อดูงบการเงิน)' in info_text:
                                company_info["financial_years_note"] = "(คลิกที่ปีเพื่อดูงบการเงิน)"
                            continue
                        elif 'ที่ตั้งสำนักงานแห่งใหญ่' in line and ':' not in line:
                            current_section = "address"
                            continue
                        elif 'Website' in line and ':' not in line:
                            current_section = "website"
                            continue
                        
                        normalized = line.replace(':', '').strip()
                        if normalized in label_map:
                            pending_label = label_map[normalized]
                            value = ""
                            if ':' in line:
                                value = line.split(':', 1)[1].strip()

                            if value:
                                company_info[pending_label] = value
                                pending_label = None
                            else:
                                if pending_label == "financial_years":
                                    company_info[pending_label] = ""
                                elif pending_label == "address":
                                    if not company_info.get("address"):
                                        company_info["address"] = ""
                                else:
                                    company_info[pending_label] = ""
                            continue

                        # แยกข้อมูลที่มี ":"
                        if ':' in line:
                            key, value = line.split(':', 1)
                            key = key.strip()
                            value = value.strip()
                            
                            if 'ประเภทนิติบุคคล' in key:
                                company_info["business_type"] = value
                            elif 'สถานะนิติบุคคล' in key:
                                company_info["status"] = value
                            elif 'ทุนจดทะเบียน' in key:
                                company_info["registered_capital"] = value
                            elif 'วันที่จดทะเบียนจัดตั้ง' in key:
                                company_info["found_date"] = value
                            elif 'เลขทะเบียนเดิม' in key:
                                company_info["old_registration_number"] = value
                            elif 'กลุ่มธุรกิจ' in key or current_section == "business_group":
                                company_info["business_group"] = value
                            elif 'ขนาดธุรกิจ' in key:
                                company_info["business_size"] = value
                            elif 'ปีที่ส่งงบการเงิน' in key or current_section == "financial_years":
                                # ดึงปีทั้งหมด
                                years = [y.strip() for y in value.split() if y.strip().isdigit()]
                                company_info["financial_years"] = ' '.join(years) if years else value
                            elif 'ที่ตั้งสำนักงานแห่งใหญ่' in key or current_section == "address":
                                company_info["address"] = value
                            elif 'Website' in key or current_section == "website":
                                company_info["website"] = value
                        else:
                            if pending_label:
                                target_key = pending_label
                                if target_key == "financial_years":
                                    existing = company_info.get(target_key, "")
                                    combined = f"{existing} {line}".strip()
                                    company_info[target_key] = combined
                                elif target_key == "address":
                                    existing = company_info.get(target_key, "")
                                    if existing:
                                        company_info[target_key] = f"{existing} {line}".strip()
                                    else:
                                        company_info[target_key] = line
                                else:
                                    if company_info.get(target_key):
                                        company_info[target_key] = f"{company_info[target_key]} {line}".strip()
                                    else:
                                        company_info[target_key] = line
                                pending_label = None
                            elif current_section == "address":
                                if company_info.get("address"):
                                    company_info["address"] += " " + line
                                else:
                                    company_info["address"] = line
                    
                    logger.info("✅ ดึงข้อมูลนิติบุคคลสำเร็จ")
            except Exception as e:
                logger.warning(f"⚠️ ไม่สามารถดึงข้อมูลนิติบุคคลได้: {str(e)}")
            
            # 3. ดึงรายชื่อกรรมการ: //*[@id="companyProfileTab1"]/div[2]/div[1]/div[2]/div
            try:
                directors_element = self.page.locator('//*[@id="companyProfileTab1"]/div[2]/div[1]/div[2]/div').first
                if await directors_element.is_visible(timeout=3000):
                    directors_text = await directors_element.inner_text()
                    company_info["directors"] = directors_text
                    company_info["directors_raw"] = directors_text

                    try:
                        list_locator = directors_element.locator('li')
                        list_count = await list_locator.count()
                        if list_count > 0:
                            list_items = []
                            for idx in range(list_count):
                                try:
                                    item_text = await list_locator.nth(idx).inner_text()
                                    if item_text:
                                        list_items.append(item_text.strip())
                                except Exception:
                                    continue
                            if list_items:
                                company_info["directors_list"] = list_items
                    except Exception:
                        pass

                    logger.info("✅ ดึงรายชื่อกรรมการสำเร็จ")
            except Exception as e:
                logger.warning(f"⚠️ ไม่สามารถดึงรายชื่อกรรมการได้: {str(e)}")
            
            # 4. ดึงข้อมูลกรรมการลงชื่อผูกพัน: //*[@id="companyProfileTab1"]/div[2]/div[1]/div[3]/div[1]
            try:
                auth_element = self.page.locator('//*[@id="companyProfileTab1"]/div[2]/div[1]/div[3]/div[1]').first
                if await auth_element.is_visible(timeout=3000):
                    auth_text = await auth_element.inner_text()
                    company_info["authorized_signatories"] = auth_text
                    logger.info("✅ ดึงข้อมูลกรรมการลงชื่อผูกพันสำเร็จ")
            except Exception as e:
                logger.warning(f"⚠️ ไม่สามารถดึงข้อมูลกรรมการลงชื่อผูกพันได้: {str(e)}")
            
            # 5. ดึงข้อมูลประเภทธุรกิจ (card-infos)
            try:
                card_infos_locator = self.page.locator('#companyProfileTab1 .card-infos')

                def assign_card_data(raw_text: str, type_key: str, objective_key: str, raw_key: str, context: str) -> bool:
                    if not raw_text:
                        return False
                    company_info[raw_key] = raw_text
                    parsed = self._parse_card_info_text(raw_text, {
                        "ประเภทธุรกิจ": "type",
                        "วัตถุประสงค์": "objective"
                    })
                    company_info[type_key] = parsed.get("type", "")
                    company_info[objective_key] = parsed.get("objective", "")
                    logger.info(f"✅ ดึง{context}สำเร็จ")
                    return True

                reg_handled = False
                latest_handled = False

                card_count = 0
                try:
                    card_count = await card_infos_locator.count()
                except Exception:
                    card_count = 0

                for idx in range(card_count):
                    try:
                        card_element = card_infos_locator.nth(idx)
                        if not await card_element.is_visible(timeout=2000):
                            continue

                        card_raw_text = ""
                        try:
                            card_raw_text = await card_element.inner_text()
                        except Exception:
                            pass

                        title_locator = card_element.locator('h5')
                        try:
                            title_count = await title_locator.count()
                        except Exception:
                            title_count = 0

                        if title_count == 0:
                            continue

                        for title_idx in range(title_count):
                            try:
                                title_element = title_locator.nth(title_idx)
                                title_text = (await title_element.inner_text()).strip()
                            except Exception:
                                continue

                            body_text = ""
                            try:
                                body_locator = title_element.locator('xpath=following-sibling::div[contains(@class, "card-body")][1]')
                                if await body_locator.count() > 0:
                                    body_text = await body_locator.inner_text()
                            except Exception:
                                pass

                            target_text = body_text or card_raw_text
                            if not target_text:
                                continue

                            if 'กรรมการลงชื่อผูกพัน' in title_text:
                                if target_text.strip() and not company_info.get("authorized_signatories"):
                                    company_info["authorized_signatories"] = target_text.strip()
                                    logger.info("✅ ดึงข้อมูลกรรมการลงชื่อผูกพันจาก card สำเร็จ")
                                continue

                            if 'ประเภทธุรกิจตอนจดทะเบียน' in title_text:
                                if assign_card_data(
                                    target_text,
                                    "business_type_registration",
                                    "business_type_registration_objective",
                                    "business_type_registration_raw",
                                    "ประเภทธุรกิจตอนจดทะเบียนจาก card"
                                ):
                                    reg_handled = True
                                continue

                            if 'ประเภทธุรกิจที่ส่งงบการเงินปีล่าสุด' in title_text:
                                if assign_card_data(
                                    target_text,
                                    "business_type_latest",
                                    "business_type_latest_objective",
                                    "business_type_latest_raw",
                                    "ประเภทธุรกิจที่ส่งงบการเงินปีล่าสุดจาก card"
                                ):
                                    latest_handled = True
                                continue
                    except Exception:
                        continue

                if not reg_handled:
                    try:
                        biz_type_reg_element = self.page.locator('//*[@id="companyProfileTab1"]/div[2]/div[1]/div[3]/div[2]').first
                        if await biz_type_reg_element.is_visible(timeout=3000):
                            biz_type_reg_text = await biz_type_reg_element.inner_text()
                            assign_card_data(
                                biz_type_reg_text,
                                "business_type_registration",
                                "business_type_registration_objective",
                                "business_type_registration_raw",
                                "ประเภทธุรกิจตอนจดทะเบียน"
                            )
                    except Exception:
                        pass

                if not latest_handled:
                    fallback_locators = [
                        'xpath=//*[@id="companyProfileTab1"]//*[normalize-space()="ประเภทธุรกิจที่ส่งงบการเงินปีล่าสุด"]/following-sibling::div[contains(@class,"card-infos")][1]',
                        '//*[@id="companyProfileTab1"]/div[2]/div[1]/div[4]/div[2]'
                    ]
                    for locator_str in fallback_locators:
                        try:
                            latest_element = self.page.locator(locator_str).first
                            if await latest_element.is_visible(timeout=3000):
                                latest_text = await latest_element.inner_text()
                                if assign_card_data(
                                    latest_text,
                                    "business_type_latest",
                                    "business_type_latest_objective",
                                    "business_type_latest_raw",
                                    "ประเภทธุรกิจที่ส่งงบการเงินปีล่าสุด"
                                ):
                                    break
                        except Exception:
                            continue
            except Exception as e:
                logger.warning(f"⚠️ ไม่สามารถดึงข้อมูลประเภทธุรกิจจาก card-infos ได้: {str(e)}")

            return company_info

        except Exception as e:
            logger.error(f"เกิดข้อผิดพลาดในการดึงข้อมูล: {str(e)}")
            return {"error": f"เกิดข้อผิดพลาดในการดึงข้อมูล: {str(e)}"}
    
    def parse_company_data(self, soup: BeautifulSoup, company_name: str) -> Dict:
        """
        แปลงข้อมูลจาก HTML เป็น Dictionary
        
        Args:
            soup (BeautifulSoup): HTML content
            company_name (str): ชื่อบริษัทที่ค้นหา
            
        Returns:
            Dict: ข้อมูลบริษัท
        """
        try:
            company_info = {
                "company_name": company_name,
                "registration_number": "",
                "business_type": "",
                "status": "",
                "registered_capital": "",
                "address": "",
                "phone": "",
                "email": "",
                "found_date": "",
                "last_update": ""
            }
            
            # ค้นหาข้อมูลจากตารางผลลัพธ์
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        
                        # แปลงข้อมูลตาม key
                        if 'เลขทะเบียน' in key or 'Registration Number' in key:
                            company_info["registration_number"] = value
                        elif 'ประเภทธุรกิจ' in key or 'Business Type' in key:
                            company_info["business_type"] = value
                        elif 'สถานะ' in key or 'Status' in key:
                            company_info["status"] = value
                        elif 'ทุนจดทะเบียน' in key or 'Registered Capital' in key:
                            company_info["registered_capital"] = value
                        elif 'ที่อยู่' in key or 'Address' in key:
                            company_info["address"] = value
                        elif 'โทรศัพท์' in key or 'Phone' in key:
                            company_info["phone"] = value
                        elif 'อีเมล' in key or 'Email' in key:
                            company_info["email"] = value
                        elif 'วันที่จดทะเบียน' in key or 'Registration Date' in key:
                            company_info["found_date"] = value
                        elif 'วันที่อัปเดต' in key or 'Last Update' in key:
                            company_info["last_update"] = value
            
            return company_info
            
        except Exception as e:
            logger.error(f"เกิดข้อผิดพลาดในการแปลงข้อมูล: {str(e)}")
            return {"error": f"เกิดข้อผิดพลาดในการแปลงข้อมูล: {str(e)}"}
    
    def format_company_info(self, company_info: Dict) -> str:
        """
        จัดรูปแบบข้อมูลบริษัทเป็นข้อความ
        
        Args:
            company_info (Dict): ข้อมูลบริษัท
            
        Returns:
            str: ข้อมูลที่จัดรูปแบบแล้ว
        """
        if "error" in company_info:
            return f"ข้อผิดพลาด: {company_info['error']}"
        
        info_parts = []
        
        if company_info.get("registration_number"):
            info_parts.append(f"เลขทะเบียน: {company_info['registration_number']}")
        
        if company_info.get("business_type"):
            info_parts.append(f"ประเภทธุรกิจ: {company_info['business_type']}")
        
        if company_info.get("status"):
            info_parts.append(f"สถานะ: {company_info['status']}")
        
        if company_info.get("registered_capital"):
            info_parts.append(f"ทุนจดทะเบียน: {company_info['registered_capital']}")
        
        if company_info.get("address"):
            info_parts.append(f"ที่อยู่: {company_info['address']}")
        
        if company_info.get("phone"):
            info_parts.append(f"โทรศัพท์: {company_info['phone']}")
        
        if company_info.get("found_date"):
            info_parts.append(f"วันที่จดทะเบียน: {company_info['found_date']}")
        
        return " | ".join(info_parts) if info_parts else "ไม่พบข้อมูล"
    

def create_dbd_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    สร้างตารางสรุปข้อมูล DBD
    
    Args:
        df (pd.DataFrame): DataFrame ที่มีข้อมูล DBD
        
    Returns:
        pd.DataFrame: ตารางสรุปข้อมูล DBD
    """
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
            
            directors_value = row.get('รายชื่อกรรมการ', '')
            if isinstance(directors_value, list):
                directors_value = " | ".join(directors_value)
            elif pd.isna(directors_value):
                directors_value = ""
            else:
                directors_value = str(directors_value).strip()

            summary_row = {
                'ชื่อบริษัท': company_name,
                'ชื่อบริษัทจาก DBD': db_company_name,
                'เลขทะเบียน': '',
                'ประเภทธุรกิจ': '',
                'สถานะ': '',
                'ทุนจดทะเบียน': '',
                'ที่อยู่': '',
                'รายชื่อกรรมการ': directors_value,
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
                elif 'รายชื่อกรรมการ:' in part:
                    summary_row['รายชื่อกรรมการ'] = part.replace('รายชื่อกรรมการ:', '').strip()
            
            summary_data.append(summary_row)
    
    return pd.DataFrame(summary_data)

def main():
    """ฟังก์ชันหลักสำหรับทดสอบ"""
    bot = DBDDataWarehouseBot()
    
    # ทดสอบการค้นหาข้อมูลบริษัท
    test_company = "ทรอเวลล์ กร"
    print(f"กำลังค้นหาข้อมูลบริษัท: {test_company}")
    
    result = bot.search_company_info(test_company)
    print("ผลลัพธ์:")
    print(result)
    
    # ทดสอบการทำความสะอาดชื่อบริษัท
    test_names = [
        "บริษัท ทรอเวลล์ กร++",
        "บจก. ดี.พราวด์ เอ็++",
        "ห้างหุ้นส่วน XYZ จำกัด"
    ]
    
    print("\nทดสอบการทำความสะอาดชื่อบริษัท:")
    for name in test_names:
        clean_name = bot.clean_company_name(name)
        print(f"'{name}' -> '{clean_name}'")

if __name__ == "__main__":
    main()