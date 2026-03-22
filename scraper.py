"""
优化版 scraper：
- 复用浏览器实例
- 浏览器崩溃自动重启
- 支持断点续跑
"""
import re
import asyncio
from playwright.async_api import async_playwright

EMAIL_RE = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

async def fetch_one(username, browser, semaphore):
    """抓取单个用户，复用 browser"""
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
    """
    批量抓取，浏览器崩溃自动重启，支持断点续跑
    每50条重启一次浏览器，防止内存泄漏
    """
    RESTART_EVERY = 50  # 每50条重启浏览器

    todo = [(idx, df.at[idx, "_username"])
            for idx in df[df["status"] == ""].index
            if df.at[idx, "_username"]]

    async with async_playwright() as p:
        browser = None

        async def get_browser():
            nonlocal browser
            if browser:
                try:
                    await browser.close()
                except:
                    pass
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--memory-pressure-off",
                ]
            )
            return browser

        await get_browser()

        # 分批处理，每批 RESTART_EVERY 条
        for batch_start in range(0, len(todo), RESTART_EVERY):
            batch = todo[batch_start: batch_start + RESTART_EVERY]

            # 如果上一批浏览器崩了，重启
            try:
                _ = browser.is_connected()
                if not browser.is_connected():
                    job["logs"].append("浏览器已断开，正在重启...")
                    await get_browser()
            except:
                job["logs"].append("浏览器异常，正在重启...")
                await get_browser()

            sem = asyncio.Semaphore(concurrency)

            async def process(idx, username):
                # 跳过已处理
                if df.at[idx, "status"] != "":
                    return
                try:
                    result = await fetch_one(username, browser, sem)
                except Exception as e:
                    result = {"username": username, "bio": "", "email": "",
                              "status": f"error: {str(e)[:60]}"}

                async with lock:
                    df.at[idx, "email"]  = result["email"]
                    df.at[idx, "status"] = result["status"]
                    job["done"] += 1
                    if result["email"]: job["found"] += 1
                    job["logs"].append(
                        f"@{username} → {result['email'] or result['status']}"
                    )
                    if len(job["logs"]) > 200:
                        job["logs"] = job["logs"][-200:]
                    # 每条实时保存
                    df.drop(columns=["_username"], errors="ignore").to_excel(
                        out_path, index=False
                    )

            await asyncio.gather(*[process(idx, uname) for idx, uname in batch])
            job["logs"].append(f"--- 已完成 {min(batch_start+RESTART_EVERY, len(todo))}/{len(todo)} 条，重启浏览器 ---")

        if browser:
            try:
                await browser.close()
            except:
                pass


async def fetch_tiktok_bio(username, semaphore):
    """兼容旧接口，供 test.py 使用"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        result = await fetch_one(username, browser, semaphore)
        await browser.close()
        return result