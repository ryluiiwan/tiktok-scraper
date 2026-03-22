"""
TikTok Email Scraper - 多用户版
- 每个用户独立密码
- 任务排队串行执行
- Railway 环境变量配置用户：USERS=alice:pw1,bob:pw2
"""
import asyncio, re, uuid, os
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
from scraper import fetch_tiktok_bio

app = FastAPI()

# ── 用户配置 ────────────────────────────────────────────────
# Railway 环境变量：USERS=alice:pass1,bob:pass2,carol:pass3
def load_users() -> dict[str, str]:
    raw = os.getenv("USERS", "")
    users = {}
    if raw:
        for pair in raw.split(","):
            pair = pair.strip()
            if ":" in pair:
                u, p = pair.split(":", 1)
                users[u.strip()] = p.strip()
    # 兜底：如果没配置 USERS，用单密码
    if not users:
        fallback = os.getenv("APP_PASSWORD", "scraper2024")
        users["user"] = fallback
    return users

USERS = load_users()

def verify_user(username: str, password: str) -> bool:
    return USERS.get(username) == password

# ── 目录 & 状态 ─────────────────────────────────────────────
UPLOAD_DIR = Path("uploads"); UPLOAD_DIR.mkdir(exist_ok=True)
RESULT_DIR = Path("results"); RESULT_DIR.mkdir(exist_ok=True)

jobs: dict[str, dict] = {}   # job_id → job
task_queue: asyncio.Queue = None

# ── 启动队列 worker ─────────────────────────────────────────
@app.on_event("startup")
async def startup():
    global task_queue
    task_queue = asyncio.Queue()
    asyncio.create_task(queue_worker())

async def queue_worker():
    while True:
        job_id = await task_queue.get()
        try:
            await run_job(job_id)
        except Exception as e:
            if job_id in jobs:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["logs"].append(f"Fatal error: {str(e)[:100]}")
        finally:
            task_queue.task_done()

# ── API: 验证用户 ───────────────────────────────────────────
@app.post("/api/verify")
async def verify(body: dict):
    ok = verify_user(body.get("username", ""), body.get("password", ""))
    return {"ok": ok}

# ── API: 上传文件并加入队列 ─────────────────────────────────
@app.post("/api/upload")
async def upload(
    file: UploadFile = File(...),
    username: str = Form(""),
    password: str = Form("")
):
    if not verify_user(username, password):
        raise HTTPException(403, "Wrong credentials")

    job_id = str(uuid.uuid4())[:8]
    path   = UPLOAD_DIR / f"{job_id}.xlsx"
    path.write_bytes(await file.read())

    df    = pd.read_excel(path)
    count = sum(1 for u in df.get("主页链接", [])
                if re.search(r'tiktok\.com/@', str(u)))

    # 计算排队位置
    running = sum(1 for j in jobs.values() if j["status"] == "running")
    queued  = sum(1 for j in jobs.values() if j["status"] == "queued")
    queue_pos = running + queued  # 0 = 立刻开始

    jobs[job_id] = {
        "status":    "queued",
        "owner":     username,
        "total":     count,
        "done":      0,
        "found":     0,
        "queue_pos": queue_pos,
        "logs":      [],
        "file":      str(path)
    }
    await task_queue.put(job_id)
    return {"job_id": job_id, "total": count, "queue_pos": queue_pos}

# ── 任务执行 ────────────────────────────────────────────────
async def run_job(job_id: str):
    job = jobs[job_id]
    job["status"]    = "running"
    job["queue_pos"] = 0

    df = pd.read_excel(job["file"])
    if "email"  not in df.columns: df["email"]  = ""
    if "status" not in df.columns: df["status"] = ""

    def get_username(url):
        m = re.search(r'tiktok\.com/@([^/?&\s]+)', str(url))
        return m.group(1) if m else ""

    df["_username"] = df["主页链接"].apply(get_username)
    todo = df[df["status"] == ""].index.tolist()
    sem  = asyncio.Semaphore(3)
    lock = asyncio.Lock()

    async def process(idx):
        uname = df.at[idx, "_username"]
        if not uname:
            return
        result = await fetch_tiktok_bio(uname, sem)
        async with lock:
            df.at[idx, "email"]  = result["email"]
            df.at[idx, "status"] = result["status"]
            job["done"] += 1
            if result["email"]: job["found"] += 1
            job["logs"].append(f"@{uname} → {result['email'] or result['status']}")
            if len(job["logs"]) > 200: job["logs"] = job["logs"][-200:]
            df.drop(columns=["_username"]).to_excel(
                RESULT_DIR / f"{job_id}_result.xlsx", index=False)

    await asyncio.gather(*[process(idx) for idx in todo])
    job["status"] = "done"

# ── API: 查询状态 ───────────────────────────────────────────
@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404)
    j = jobs[job_id]
    # 实时更新排队位置
    if j["status"] == "queued":
        pos = 0
        for jid, jdata in jobs.items():
            if jdata["status"] == "running": pos += 1
            elif jdata["status"] == "queued":
                if jid == job_id: break
                pos += 1
        j["queue_pos"] = pos
    return {
        "status":    j["status"],
        "total":     j["total"],
        "done":      j["done"],
        "found":     j["found"],
        "queue_pos": j.get("queue_pos", 0),
        "logs":      j["logs"][-20:]
    }

# ── API: 下载结果 ───────────────────────────────────────────
@app.get("/api/download/{job_id}")
async def download(job_id: str):
    path = RESULT_DIR / f"{job_id}_result.xlsx"
    if not path.exists():
        raise HTTPException(404, "Result not ready")
    return FileResponse(
        path, filename="tiktok_emails_result.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

app.mount("/", StaticFiles(directory="static", html=True), name="static")