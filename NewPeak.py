import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from typing import Callable, Optional, Dict, Any, List, Tuple

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NewPeakBot:
    """Bot สำหรับงานกรอกข้อมูลบน https://secure.peakaccount.com (แยกจาก PeakEngineBot)"""

    def __init__(self, use_browser: bool = True, headless: bool = False) -> None:
        self.base_url = "https://secure.peakaccount.com"
        self.login_url = self.base_url
        self.use_browser = use_browser
        self.headless = headless
        self.browser = None
        self.page = None
        self.playwright = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self.is_logged_in = False

        self.link_company = "https://secure.peakaccount.com/home?emi=MzIwNjE5"
        self.link_receipt = "https://secure.peakaccount.com/income/receiptCreate?emi=MzIwNjE5"

        if use_browser:
            self._start_browser()

    @staticmethod
    def _parse_amount(raw: Any) -> Optional[float]:
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return None
        text = str(raw).strip()
        if not text:
            return None
        negative = False
        if text.startswith("(") and text.endswith(")"):
            negative = True
            text = text[1:-1]
        text = text.replace(",", "").replace("+", "").replace(" ", "")
        try:
            value = float(text)
            return -value if negative else value
        except ValueError:
            return None

    @staticmethod
    def _determine_url(transfer_type: str, has_dbd: bool) -> str:
        normalized = (transfer_type or "").strip()
        lower = normalized.lower()
        receipt_url = "https://secure.peakaccount.com/income/receiptCreate?emi=MzIwNjE5"
        deposit_url = "https://secure.peakaccount.com/income/depositCreate?emi=MzIwNjE5"
        if any(keyword in lower for keyword in ["บริษัท", "บจก", "limited", "co.", "company"]):
            return receipt_url if has_dbd else deposit_url
        if any(keyword in lower for keyword in ["ห้างหุ้นส่วน", "หจก", "partnership"]):
            return receipt_url if has_dbd else deposit_url
        if any(keyword in lower for keyword in ["บุคคล", "บุคคลธรรมดา", "บุคคลทั่วไป", "person", "individual"]):
            return deposit_url
        return ""

    @staticmethod
    def _has_dbd_info(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, float) and pd.isna(value):
            return False
        return bool(str(value).strip())

    @staticmethod
    def _extract_registration(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value)
        match = re.search(r"เลขทะเบียน[:\s]*([0-9\-]+)", text)
        if match:
            digits = "".join(ch for ch in match.group(1) if ch.isdigit())
            if len(digits) >= 13:
                return digits[:13]
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) >= 13:
            return digits[:13]
        match = re.search(r"เลขทะเบียน[:\s]*([0-9\-]+)", text)
        if match:
            digits = "".join(ch for ch in match.group(1) if ch.isdigit())
            if len(digits) >= 13:
                return digits[:13]
        return None

    @staticmethod
    def _parse_dbd_details(value: Any) -> Dict[str, str]:
        details: Dict[str, str] = {}
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return details
        raw_text = str(value).strip()
        if not raw_text or raw_text.lower() == "none":
            return details

        parts = [part.strip() for part in raw_text.split("|") if part.strip()]
        for part in parts:
            if ":" not in part:
                continue
            key, val = part.split(":", 1)
            key = key.strip()
            val = val.strip()
            if key and val:
                details[key] = val
        return details

    async def _iter_frame_contexts(self):
        """
        รวม page หลักและทุก iframe ที่พร้อมใช้งาน เพื่อใช้ค้นหา element
        """
        if not self.page:
            return []

        contexts = []
        try:
            contexts.append(("page", self.page))
        except Exception:
            return []

        try:
            for frame in self.page.frames:
                identifier = frame.name or frame.url or "frame"
                contexts.append((f"frame:{identifier}", frame))
        except Exception:
            pass

        return contexts

    async def _find_element_any_frame(
        self,
        selectors: List[str],
        *,
        timeout: int = 3000,
        state: Optional[str] = "visible",
        log: Optional[Callable[[str, str], None]] = None,
    ):
        """
        พยายามค้นหา selector ในทุก frame/page ที่สามารถเข้าถึงได้
        """
        contexts = await self._iter_frame_contexts()
        if not contexts:
            return None, None

        last_error: Optional[Exception] = None

        for selector in selectors:
            for ctx_name, ctx in contexts:
                try:
                    element = await ctx.wait_for_selector(selector, timeout=timeout, state=state)
                    if element:
                        if log:
                            log(f"🔍 พบ element '{selector}' ใน {ctx_name}", "info")
                        return element, ctx
                except Exception as exc:
                    last_error = exc
                    continue

        if log and selectors:
            log(
                f"⚠️ ไม่พบ element จาก selectors: {selectors}. Last error: {last_error}",
                "warning",
            )
        return None, None

    @staticmethod
    def _parse_document_date(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, pd.Timestamp):
            return value.strftime("%d/%m/%Y")
        if isinstance(value, (datetime, date)):
            return value.strftime("%d/%m/%Y")
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")
        except Exception:
            parsed = None
        if parsed is None or pd.isna(parsed):
            return None
        return parsed.strftime("%d/%m/%Y")

    def _build_transaction_tasks(
        self,
        df: pd.DataFrame,
        amount_column: str,
        type_column: str,
        dbd_column: str,
        date_column: Optional[str] = None,
        company_column: Optional[str] = "ชื่อบริษัทจาก DBD",
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        tasks: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []

        for index, row in df.iterrows():
            row_number = index + 1

            amount = self._parse_amount(row.get(amount_column))
            if amount is None:
                skipped.append(
                    {
                        "row_number": row_number,
                        "reason": "amount_invalid",
                        "details": str(row.get(amount_column)),
                    }
                )
                continue
            if amount < 0:
                skipped.append(
                    {
                        "row_number": row_number,
                        "reason": "amount_negative",
                        "details": str(row.get(amount_column)),
                    }
                )
                continue

            transfer_type = str(row.get(type_column) or "").strip()
            if not transfer_type:
                skipped.append(
                    {
                        "row_number": row_number,
                        "reason": "missing_transfer_type",
                        "details": "",
                    }
                )
                continue

            dbd_value = row.get(dbd_column)
            dbd_has_data = self._has_dbd_info(dbd_value)
            target_url = self._determine_url(transfer_type, dbd_has_data)
            if not target_url:
                skipped.append(
                    {
                        "row_number": row_number,
                        "reason": "unknown_transfer_type",
                        "details": transfer_type,
                    }
                )
                continue

            registration_number = self._extract_registration(dbd_value)
            document_date = None
            if date_column:
                document_date = self._parse_document_date(row.get(date_column))

            dbd_details = self._parse_dbd_details(dbd_value)
            company_name = ""
            if company_column:
                company_name = str(row.get(company_column) or "").strip()

            tasks.append(
                {
                    "row_index": index,
                    "row_number": row_number,
                    "amount": amount,
                    "transfer_type": transfer_type,
                    "dbd_has_data": dbd_has_data,
                    "registration": registration_number,
                    "target_url": target_url,
                    "document_date": document_date,
                    "dbd_raw": dbd_value,
                    "dbd_details": dbd_details,
                    "company_name": company_name,
                }
            )

        return tasks, skipped

    def prepare_transaction_tasks(
        self,
        df: pd.DataFrame,
        amount_column: str = "จำนวนเงิน",
        type_column: str = "ประเภทผู้ส่งโอน",
        dbd_column: str = "ข้อมูล DBD",
        date_column: Optional[str] = "วันที่",
        company_column: Optional[str] = "ชื่อบริษัทจาก DBD",
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        return self._build_transaction_tasks(
            df,
            amount_column,
            type_column,
            dbd_column,
            date_column,
            company_column,
        )

    def preview_excel_transactions(
        self,
        df: pd.DataFrame,
        amount_column: str = "จำนวนเงิน",
        type_column: str = "ประเภทผู้ส่งโอน",
        dbd_column: str = "ข้อมูล DBD",
        date_column: Optional[str] = "วันที่",
        company_column: Optional[str] = "ชื่อบริษัทจาก DBD",
    ) -> Dict[str, pd.DataFrame]:
        tasks, skipped = self.prepare_transaction_tasks(
            df,
            amount_column,
            type_column,
            dbd_column,
            date_column,
            company_column,
        )
        ready_df = pd.DataFrame(tasks)
        skipped_df = pd.DataFrame(skipped)
        return {"ready": ready_df, "skipped": skipped_df}

    def _start_browser(self) -> None:
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise RuntimeError(
                "ไม่สามารถ import Playwright ได้ กรุณาติดตั้งด้วยคำสั่ง `pip install playwright` "
                "และรัน `playwright install chromium`"
            ) from exc

        logger.info("🚀 กำลังเริ่มต้น Playwright สำหรับ NewPeakBot")

        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="newpeak_playwright")

        def init_playwright_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def async_init():
                logger.info("🌐 เริ่มต้น async_playwright สำหรับ NewPeakBot")
                pw = await async_playwright().start()
                browser = await pw.chromium.launch(
                    headless=self.headless,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--no-sandbox",
                        "--start-maximized",
                    ],
                )
                context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    screen={"width": 1920, "height": 1080},
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                )
                page = await context.new_page()
                return pw, browser, page

            return loop.run_until_complete(async_init())

        try:
            future = self._executor.submit(init_playwright_in_thread)
            self.playwright, self.browser, self.page = future.result(timeout=60)
            logger.info("✅ เปิด Browser สำหรับ NewPeakBot สำเร็จ")
        except Exception as exc:
            logger.exception("❌ เปิด Browser สำหรับ NewPeakBot ไม่สำเร็จ")
            raise RuntimeError("ไม่สามารถเปิด Browser ได้") from exc

    def _run_async(self, coro_callable, timeout: int = 60):
        if not self._executor:
            raise RuntimeError("Thread executor ยังไม่ได้เริ่มต้น")

        def runner():
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    raise RuntimeError
            except Exception:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            return loop.run_until_complete(coro_callable())

        return self._executor.submit(runner).result(timeout=timeout)

    def login(
        self,
        username: str,
        password: str,
        navigate_after_login: bool = True,
        log_callback: Optional[Callable[[str, str], None]] = None,
    ) -> bool:
        """
        Login เข้า https://secure.peakaccount.com และ (ถ้าต้องการ) ไปยังลิงก์บริษัท/ใบเสร็จใหม่
        """

        def log(message: str, status: str = "info") -> None:
            if log_callback:
                try:
                    log_callback(message, status)
                except Exception:
                    pass
            getattr(logger, "info" if status == "info" else status, logger.info)(message)

        if not self.use_browser or not self.page:
            log("❌ Browser ยังไม่ได้เปิด", "error")
            return False

        async def async_login():
            try:
                log("🌐 กำลังเปิดหน้า Login (peakaccount.com)", "info")
                await self.page.goto(self.login_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(0.1)

                email_selectors = [
                    "#ugxuj",
                    'input[name="email"]',
                    'input[placeholder*="อีเมล"]',
                ]
                password_selectors = [
                    "#91c31l",
                    'input[name="password"]',
                    'input[type="password"]',
                ]

                email_input = None
                selector_timeout = 2000

                for selector in email_selectors:
                    try:
                        email_input = await self.page.wait_for_selector(
                            selector, timeout=selector_timeout, state="visible"
                        )
                        if email_input:
                            log(f"✅ พบช่อง Email ด้วย selector: {selector}", "success")
                            break
                    except Exception:
                        continue

                if not email_input:
                    log("❌ ไม่พบช่องกรอก Email", "error")
                    return False

                await email_input.click()
                await email_input.fill(username)

                password_input = None
                for selector in password_selectors:
                    try:
                        password_input = await self.page.wait_for_selector(
                            selector, timeout=selector_timeout, state="visible"
                        )
                        if password_input:
                            log(f"✅ พบช่อง Password ด้วย selector: {selector}", "success")
                            break
                    except Exception:
                        continue

                if not password_input:
                    log("❌ ไม่พบช่องกรอก Password", "error")
                    return False

                await password_input.click()
                await password_input.fill(password)

                login_button_selectors = [
                    'button:has-text("เข้าสู่ระบบ PEAK")',
                    'button[type="submit"]',
                    'button[data-v-aafa16b8]',
                ]

                login_button = None
                for selector in login_button_selectors:
                    try:
                        login_button = await self.page.wait_for_selector(
                            selector, timeout=selector_timeout + 1000, state="visible"
                        )
                        if login_button:
                            log(f"✅ พบปุ่มเข้าสู่ระบบด้วย selector: {selector}", "success")
                            break
                    except Exception:
                        continue

                if not login_button:
                    log("❌ ไม่พบปุ่มเข้าสู่ระบบ", "error")
                    return False

                await login_button.click()
                log("🔘 กำลังกดปุ่มเข้าสู่ระบบ...", "info")
                await asyncio.sleep(0.2)
                try:
                    await self.page.wait_for_load_state("domcontentloaded", timeout=12000)
                except Exception:
                    log("⚠️ รอ domcontentloaded ไม่สำเร็จ แต่จะตรวจสอบ URL ต่อ", "warning")
                try:
                    await self.page.wait_for_url(
                        lambda url: "login" not in url.lower(),
                        timeout=12000,
                    )
                except Exception:
                    log("⚠️ URL ยังไม่เปลี่ยนหลัง Login ภายในเวลาที่กำหนด", "warning")
                current_url = self.page.url
                log(f"📍 URL หลัง Login: {current_url}", "info")

                if "login" in current_url.lower():
                    log("⚠️ ยังอยู่หน้า Login ตรวจสอบ username/password", "warning")
                    self.is_logged_in = False
                    return False

                self.is_logged_in = True
                log("✅ Login สำเร็จ", "success")

                confirm_selectors = [
                    'button:has-text("ยืนยันการเข้าสู่ระบบ")',
                    'button[data-v-aafa16b8]:has-text("ยืนยันการเข้าสู่ระบบ")',
                    '//button[contains(normalize-space(), "ยืนยันการเข้าสู่ระบบ")]',
                ]

                confirm_clicked = False

                for selector in confirm_selectors:
                    if confirm_clicked:
                        break
                    try:
                        if selector.startswith("//"):
                            confirm_button = self.page.locator(selector)
                            await confirm_button.wait_for(state="visible", timeout=2000)
                            if await confirm_button.count() > 0:
                                log("✅ พบปุ่มยืนยันการเข้าสู่ระบบ (XPath)", "success")
                                await confirm_button.first.click()
                                log("🔘 คลิกปุ่มยืนยันการเข้าสู่ระบบสำเร็จ", "success")
                                await asyncio.sleep(0.3)
                                confirm_clicked = True
                                if navigate_after_login:
                                    log("➡️ หลังยืนยัน นำทางไปยังลิงก์ที่ตั้งค่าไว้", "info")
                                    await self._navigate_post_login(log)
                                break
                        else:
                            confirm_button = await self.page.wait_for_selector(selector, timeout=2000, state="visible")
                            if confirm_button:
                                log("✅ พบปุ่มยืนยันการเข้าสู่ระบบ", "success")
                                await confirm_button.click()
                                log("🔘 คลิกปุ่มยืนยันการเข้าสู่ระบบสำเร็จ", "success")
                                await asyncio.sleep(0.3)
                                confirm_clicked = True
                                if navigate_after_login:
                                    log("➡️ หลังยืนยัน นำทางไปยังลิงก์ที่ตั้งค่าไว้", "info")
                                    await self._navigate_post_login(log)
                                break
                    except Exception:
                        continue

                if navigate_after_login and not confirm_clicked:
                    await self._navigate_post_login(log)

                return True
            except Exception as exc:
                log(f"❌ เกิดข้อผิดพลาดระหว่าง Login: {exc}", "error")
                self.is_logged_in = False
                return False

        return self._run_async(async_login, timeout=120)

    async def _navigate_post_login(self, log: Callable[[str, str], None]) -> None:
        if self.link_company:
            try:
                log(f"🌐 กำลังไปที่ Link_compay_newpeak: {self.link_company}", "info")
                await self.page.goto(self.link_company, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(0.15)
                log("✅ โหลดหน้า Link_compay_newpeak สำเร็จ", "success")
            except Exception as exc:
                log(f"⚠️ ไปหน้า Link_compay_newpeak ไม่สำเร็จ: {exc}", "warning")

        if self.link_receipt:
            try:
                log(f"🌐 กำลังไปที่ Link_receipt_newpeak: {self.link_receipt}", "info")
                await self.page.goto(self.link_receipt, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(0.15)
                log("✅ โหลดหน้า Link_receipt_newpeak สำเร็จ", "success")
            except Exception as exc:
                log(f"⚠️ ไปหน้า Link_receipt_newpeak ไม่สำเร็จ: {exc}", "warning")

    def close(self) -> None:
        if not self._executor:
            return

        def close_runner():
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    raise RuntimeError
            except Exception:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            async def async_close():
                if self.page:
                    await self.page.close()
                if self.browser:
                    await self.browser.close()
                if self.playwright:
                    await self.playwright.stop()

            return loop.run_until_complete(async_close())

        try:
            self._executor.submit(close_runner).result(timeout=20)
            logger.info("✅ ปิด Browser สำหรับ NewPeakBot สำเร็จ")
        except Exception:
            logger.exception("⚠️ ปิด Browser สำหรับ NewPeakBot ไม่สำเร็จ")
        finally:
            self._executor.shutdown(wait=True)
            self._executor = None

    def process_excel_transactions(
        self,
        df: pd.DataFrame,
        amount_column: str = "จำนวนเงิน",
        type_column: str = "ประเภทผู้ส่งโอน",
        dbd_column: str = "ข้อมูล DBD",
        date_column: Optional[str] = "วันที่",
        company_column: Optional[str] = "ชื่อบริษัทจาก DBD",
        log_callback: Optional[Callable[[str, str], None]] = None,
        prepared_tasks: Optional[List[Dict[str, Any]]] = None,
        skipped_info: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        def log(message: str, status: str = "info") -> None:
            if log_callback:
                try:
                    log_callback(message, status)
                except Exception:
                    pass
            getattr(logger, "info" if status == "info" else status, logger.info)(message)

        if not self.use_browser or not self.page:
            log("❌ Browser ยังไม่ได้เปิด", "error")
            return {"error": "browser_not_ready"}
        if not self.is_logged_in:
            log("❌ ยังไม่ได้เข้าสู่ระบบ New Peak", "error")
            return {"error": "not_logged_in"}
        if df is None or df.empty:
            log("⚠️ DataFrame ว่าง ไม่มีข้อมูลให้ประมวลผล", "warning")
            return {"processed": 0, "skipped": 0, "errors": []}

        log(
            "🧾 เริ่มประมวลผลข้อมูล New Peak "
            f"(rows={len(df)}, amount_col='{amount_column}', type_col='{type_column}', "
            f"dbd_col='{dbd_column}', date_col='{date_column}')",
            "info",
        )

        tasks = prepared_tasks
        skipped_records = skipped_info or []
        if tasks is None:
            tasks, skipped_records = self._build_transaction_tasks(
                df,
                amount_column,
                type_column,
                dbd_column,
                date_column,
                company_column,
            )

        log(
            f"📋 รายการที่พร้อมใช้งาน {len(tasks)} รายการ / ข้าม {len(skipped_records)}",
            "info",
        )

        async def async_process() -> Dict[str, Any]:
            processed = 0
            errors = []

            for skip in skipped_records:
                row_number = skip.get("row_number")
                reason = skip.get("reason", "")
                details = skip.get("details", "")
                status = "warning" if reason != "amount_negative" else "info"
                log(f"ℹ️ ข้ามแถว {row_number}: {reason} {details}", status)

            if not tasks:
                log("ℹ️ ไม่มีรายการที่ผ่านเงื่อนไขสำหรับ New Peak", "info")
                return {
                    "processed": 0,
                    "skipped": len(skipped_records),
                    "skipped_details": skipped_records,
                    "errors": [],
                }

            for task in tasks:
                row_number = task.get("row_number", 0)
                target_url = task.get("target_url", "")
                registration_number = task.get("registration")
                document_date = task.get("document_date")

                try:
                    log(f"➡️ ({row_number}) ไปยังหน้า: {target_url}", "info")
                    await self.page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(0.1)

                    if document_date:
                        log(f"🗓️ ({row_number}) เตรียมกรอกวันที่ออก: {document_date}", "info")
                        date_selectors = [
                            'input[name="วันที่ออก"]',
                            'input[name="documentDate"]',
                            'input[name="issueDate"]',
                            'input[placeholder*="วันที่ออก"]',
                            'input[data-qa*="document-date"]',
                        ]
                        date_input, date_context = await self._find_element_any_frame(
                            date_selectors,
                            timeout=4000,
                            state="visible",
                            log=log,
                        )
                        if date_input:
                            try:
                                if date_context and hasattr(date_context, "locator"):
                                    try:
                                        date_locator = date_context.locator('input[name="วันที่ออก"]')
                                        locator_first = date_locator.first
                                        await locator_first.scroll_into_view_if_needed()
                                    except Exception:
                                        pass
                                await date_input.click()
                                try:
                                    await date_input.fill("")
                                except Exception:
                                    pass
                                try:
                                    await date_input.type(document_date, delay=20)
                                except Exception:
                                    await date_input.fill(document_date)
                                await asyncio.sleep(0.2)
                                log(f"✅ ({row_number}) กรอกวันที่ออก {document_date}", "success")
                            except Exception as date_exc:
                                log(f"⚠️ แถว {row_number} กรอกวันที่ออกไม่สำเร็จ: {date_exc}", "warning")
                        else:
                            log(f"⚠️ แถว {row_number} ไม่พบช่องสำหรับกรอกวันที่ออก", "warning")

                    if registration_number:
                        log(f"🆔 ({row_number}) เตรียมกรอกเลขทะเบียน: {registration_number}", "info")
                        candidate_selectors = [
                            '#inputDropdownDataList input.textSelectedDropdown[placeholder*="พิมพ์เพื่อค้นหาผู้ติดต่อ"]',
                            '#inputDropdownDataList input.textSelectedDropdown',
                            '#inputDropdownDataList input[placeholder*="พิมพ์เพื่อค้นหาผู้ติดต่อ"]',
                            '#inputDropdownDataList input[placeholder*="ค้นหาผู้ติดต่อ"]',
                            '#inputDropdownDataList input',
                        ]
                        container_selectors = [
                            '#inputDropdownDataList div.selectInputDropdown',
                            '#inputDropdownDataList div.dropdown',
                            '#inputDropdownDataList',
                        ]

                        active_input = None
                        active_container = None

                        for sel in container_selectors:
                            locator = self.page.locator(sel)
                            if await locator.count() > 0:
                                active_container = locator.first
                                break

                        if active_container:
                            try:
                                await active_container.scroll_into_view_if_needed()
                                await active_container.click()
                            except Exception:
                                pass

                        for selector in candidate_selectors:
                            locator = self.page.locator(selector)
                            try:
                                await locator.first.wait_for(state="visible", timeout=1500)
                                if await locator.count() > 0:
                                    active_input = locator.first
                                    log(f"🔍 ({row_number}) ใช้ selector '{selector}' เพื่อกรอกเลขทะเบียน", "info")
                                    break
                            except Exception:
                                continue

                        if active_input:
                            try:
                                await active_input.click()
                                pasted = False
                                try:
                                    await self.page.evaluate(
                                        "(text) => navigator.clipboard.writeText(text)",
                                        registration_number,
                                    )
                                    await self.page.keyboard.press("Control+V")
                                    pasted = True
                                except Exception:
                                    pasted = False
                                if not pasted:
                                    try:
                                        await self.page.keyboard.insert_text(registration_number)
                                        pasted = True
                                    except Exception:
                                        pasted = False
                                if not pasted:
                                    await active_input.fill(registration_number)
                                log(
                                    f"✅ ({row_number}) กรอกเลขทะเบียน {registration_number} ในช่องค้นหาผู้ติดต่อ",
                                    "success",
                                )
                            except Exception as fill_exc:
                                log(f"⚠️ แถว {row_number} กรอกเลขทะเบียนในช่องค้นหาผู้ติดต่อไม่สำเร็จ: {fill_exc}", "warning")
                        else:
                            log(f"⚠️ แถว {row_number} ไม่พบช่องกรอกค้นหาผู้ติดต่อเพื่อกรอกเลขทะเบียน", "warning")

                    processed += 1
                except Exception as exc:
                    error_msg = f"แถว {row_number} เข้า URL ไม่สำเร็จ: {exc}"
                    log(f"❌ {error_msg}", "error")
                    errors.append({"row": row_number, "error": str(exc)})

            return {
                "processed": processed,
                "skipped": len(skipped_records),
                "skipped_details": skipped_records,
                "errors": errors,
            }

        return self._run_async(async_process, timeout=600)
