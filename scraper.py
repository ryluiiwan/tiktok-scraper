import re
import asyncio
from playwright.async_api import async_playwright

EMAIL_RE = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

async def fetch_tiktok_bio(username: str, semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
                locale="en-US"
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page = await ctx.new_page()
            try:
                await page.goto(
                    f"https://www.tiktok.com/@{username}",
                    wait_until="domcontentloaded",
                    timeout=30000
                )
                await asyncio.sleep(3)
                bio = ""
                for selector in ['[data-e2e="user-bio"]', 'h2[data-e2e="user-bio"]']:
                    try:
                        el = page.locator(selector).first
                        if await el.is_visible(timeout=5000):
                            bio = await el.text_content() or ""
                            if bio.strip():
                                break
                    except:
                        pass
                if not bio:
                    bio = (await page.evaluate("()=>document.body.innerText"))[:2000]
                emails = list(set(re.findall(EMAIL_RE, bio)))
                return {"username": username, "bio": bio.strip()[:300],
                        "email": "; ".join(emails),
                        "status": "found" if emails else "no_email"}
            except Exception as e:
                return {"username": username, "bio": "", "email": "",
                        "status": f"error: {str(e)[:80]}"}
            finally:
                await browser.close()
