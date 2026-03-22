"""
优化版 scraper：整个任务只启动一次浏览器
速度提升 3-4 倍
"""
import re
import asyncio
from playwright.async_api import async_playwright, Browser

EMAIL_RE = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

async def fetch_bio_with_browser(username, browser, semaphore):
    async with semaphore:
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/17.0 Mobile/15E148 Safari/604.1"
            ),
            locale="en-US"
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        page = await ctx.new_page()
        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf}",
            lambda route: route.abort()
        )
        try:
            await page.goto(
                f"https://www.tiktok.com/@{username}",
                wait_until="domcontentloaded",
                timeout=25000
            )
            await asyncio.sleep(2)

            bio = ""
            for selector in ['[data-e2e="user-bio"]', 'h2[data-e2e="user-bio"]']:
                try:
                    el = page.locator(selector).first
                    if await el.is_visible(timeout=4000):
                        bio = await el.text_content(timeout=4000) or ""
                        if bio.strip():
                            break
                except:
                    pass

            if not bio:
                bio = (await page.evaluate("()=>document.body.innerText"))[:2000]

            emails = list(set(re.findall(EMAIL_RE, bio)))
            return {
                "username": username,
                "bio":      bio.strip()[:300],
                "email":    "; ".join(emails),
                "status":   "found" if emails else "no_email"
            }
        except Exception as e:
            return {"username": username, "bio": "", "email": "",
                    "status": f"error: {str(e)[:80]}"}
        finally:
            await ctx.close()


async def batch_fetch(usernames, job, df, out_path, lock, concurrency=2):
    """复用单个浏览器实例处理所有用户，供 main.py 调用"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-gpu",

            ]
        )
        sem = asyncio.Semaphore(concurrency)

        async def process(idx, username):
            if not username:
                return
            result = await fetch_bio_with_browser(username, browser, sem)
            async with lock:
                df.at[idx, "email"]  = result["email"]
                df.at[idx, "status"] = result["status"]
                job["done"] += 1
                if result["email"]: job["found"] += 1
                job["logs"].append(f"@{username} → {result['email'] or result['status']}")
                if len(job["logs"]) > 200: job["logs"] = job["logs"][-200:]
                df.drop(columns=["_username"], errors="ignore").to_excel(out_path, index=False)

        todo = [(idx, df.at[idx, "_username"]) for idx in df[df["status"] == ""].index]
        await asyncio.gather(*[process(idx, uname) for idx, uname in todo])
        await browser.close()


async def fetch_tiktok_bio(username, semaphore):
    """兼容旧接口，供 test.py 使用"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", ]
        )
        result = await fetch_bio_with_browser(username, browser, semaphore)
        await browser.close()
        return result