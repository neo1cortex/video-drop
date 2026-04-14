import os
import uuid
import json
import time
import asyncio
import subprocess
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("video-drop")

API_KEY = os.environ.get("API_KEY", "")

app = FastAPI()

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8093")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/downloads/finished"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/downloads/tmp"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "1"))
MAX_JOB_AGE = int(os.getenv("MAX_JOB_AGE", "3600"))

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

jobs: dict = {}
semaphore = asyncio.Semaphore(MAX_CONCURRENT)
queue_size = 0

COMPATIBLE_VIDEO = {"h264", "avc", "avc1"}
COMPATIBLE_AUDIO = {"aac", "mp3", "m4a", "mp4a"}

PREFERRED_FORMAT = (
    "bestvideo[ext=mp4][vcodec^=avc]+bestaudio[ext=m4a]"
    "/bestvideo[ext=mp4]+bestaudio[ext=m4a]"
    "/bestvideo+bestaudio"
    "/best"
)

FAST_ORIGINS = {
    "instagram.com",
    "cdninstagram.com",
    "tiktok.com",
    "facebook.com",
    "fbcdn.net",
}


class JobIn(BaseModel):
    url: str
    format: str = "mp4"
    mobile: bool = True
    timeout_dl: int = 300
    timeout_ff: int = 600


def is_fast_origin(url: str) -> bool:
    return any(domain in url for domain in FAST_ORIGINS)


def needs_reencode(filepath: str) -> bool:
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                "-show_format",
                str(filepath),
            ],
            capture_output=True, text=True, timeout=15,
        )
        data = json.loads(result.stdout or "{}")

        fmt = data.get("format", {}).get("format_name", "")
        if "mp4" not in fmt and "mov" not in fmt:
            return True

        streams = data.get("streams", [])
        if not streams:
            return True

        for stream in streams:
            codec = stream.get("codec_name", "").lower()
            ctype = stream.get("codec_type", "")

            if not codec or ctype not in ("video", "audio"):
                continue

            if ctype == "video" and codec not in COMPATIBLE_VIDEO:
                return True
            if ctype == "audio" and codec not in COMPATIBLE_AUDIO:
                return True

        return False
    except Exception:
        return True


@app.middleware("http")
async def verify_api_key(request: Request, call_next):
    if request.url.path == "/health":
        return await call_next(request)

    key = request.headers.get("X-API-Key")
    if not key or key != API_KEY:
        return JSONResponse(status_code=401, content={"error": "Unauthorized"})

    return await call_next(request)


@app.on_event("startup")
async def startup():
    asyncio.create_task(cleanup_old_jobs())


async def cleanup_old_jobs():
    while True:
        await asyncio.sleep(600)
        now = time.time()
        to_delete = [
            jid for jid, j in list(jobs.items())
            if j.get("finished_at") and now - j["finished_at"] > MAX_JOB_AGE
        ]
        for jid in to_delete:
            filename = jobs[jid].get("filename")
            if filename:
                (DOWNLOAD_DIR / filename).unlink(missing_ok=True)
            del jobs[jid]


@app.get("/health")
def health():
    return {"status": "ok", "jobs": len(jobs), "queued": queue_size}


@app.post("/api/jobs")
async def create_job(job: JobIn):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "id": job_id,
        "url": job.url,
        "status": "queued",
        "filename": None,
        "download_url": None,
        "error": None,
        "queue_position": None,
        "created_at": time.time(),
        "finished_at": None,
        "timings": {},
    }
    asyncio.create_task(run_job(job_id, job))
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(content=job)


@app.get("/api/jobs/{job_id}/wait")
async def wait_for_job(job_id: str, timeout: int = 55):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    deadline = time.time() + timeout
    while time.time() < deadline:
        job = jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        if job["status"] in ("finished", "error"):
            return JSONResponse(content=job)
        await asyncio.sleep(0.8)

    return JSONResponse(
        status_code=200,
        content={
            **(jobs.get(job_id, {})),
            "wait_timeout": True,
        }
    )


@app.get("/files/{filename}")
def get_file(filename: str):
    path = (DOWNLOAD_DIR / filename).resolve()
    if not str(path).startswith(str(DOWNLOAD_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=filename, media_type="application/octet-stream")


async def run_job(job_id: str, job: JobIn):
    global queue_size

    t_total = time.time()
    timings = {}

    queue_size += 1
    jobs[job_id]["queue_position"] = queue_size

    async with semaphore:
        queue_size -= 1
        jobs[job_id]["queue_position"] = None
        jobs[job_id]["status"] = "downloading"

        raw_outtmpl = str(TMP_DIR / f"{job_id}_raw.%(ext)s")
        fast = is_fast_origin(job.url)

        cmd_dl = [
            "yt-dlp",
            "--cookies", "/code/cookies/instagram.txt",
            "--no-playlist",
            "--restrict-filenames",
            "--merge-output-format", "mp4",
            "-f", PREFERRED_FORMAT,
            "--no-part",
            "-4",
        ]

        if not fast:
            cmd_dl += ["--concurrent-fragments", "4"]

        cmd_dl += ["-o", raw_outtmpl, job.url]

        log.info("[%s] Iniciando descarga - origen rapido: %s", job_id[:8], fast)

        try:
            t0 = time.time()
            proc = await asyncio.create_subprocess_exec(
                *cmd_dl,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=job.timeout_dl
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = f"Timeout: descarga supero {job.timeout_dl}s"
                jobs[job_id]["finished_at"] = time.time()
                return

            timings["download_s"] = round(time.time() - t0, 2)
            log.info("[%s] Descarga completada en %.2fs", job_id[:8], timings["download_s"])

            if proc.returncode != 0:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = stderr.decode(errors="ignore")[-1000:]
                jobs[job_id]["finished_at"] = time.time()
                return

            candidates = sorted(TMP_DIR.glob(f"{job_id}_raw.*"))
            if not candidates:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"] = "Archivo descargado no encontrado"
                jobs[job_id]["finished_at"] = time.time()
                return

            src = candidates[0]
            final_name = f"video-{job_id[:8]}.mp4"
            dst = DOWNLOAD_DIR / final_name

            jobs[job_id]["status"] = "processing"
            t1 = time.time()

            if fast:
                log.info("[%s] Fast path reparado: ffmpeg copy + faststart + genpts", job_id[:8])
                cmd_ff = [
                    "ffmpeg", "-y",
                    "-hwaccel", "vaapi",
                    "-hwaccel_device", "/dev/dri/renderD128",
                    "-hwaccel_output_format", "vaapi",
                    "-ss", "0.3",          # ← salta frames negros iniciales
                    "-i", str(src),
                    "-vf", "format=nv12|vaapi,hwupload",
                    "-c:v", "h264_vaapi",
                    "-qp", "23",
                    "-c:a", "aac",
                    "-b:a", "128k",
                    "-movflags", "+faststart",
                    str(dst),
                ]

                proc2 = await asyncio.create_subprocess_exec(
                    *cmd_ff,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                try:
                    stdout2, stderr2 = await asyncio.wait_for(
                        proc2.communicate(), timeout=job.timeout_ff
                    )
                except asyncio.TimeoutError:
                    proc2.kill()
                    await proc2.communicate()
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"] = f"Timeout: reparacion supero {job.timeout_ff}s"
                    jobs[job_id]["finished_at"] = time.time()
                    src.unlink(missing_ok=True)
                    return

                if proc2.returncode != 0:
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"] = stderr2.decode(errors="ignore")[-1000:]
                    jobs[job_id]["finished_at"] = time.time()
                    src.unlink(missing_ok=True)
                    return

                src.unlink(missing_ok=True)
                timings["ffprobe_s"] = 0
                timings["ffmpeg_s"] = round(time.time() - t1, 2)
                log.info("[%s] ffmpeg: %.2fs", job_id[:8], timings["ffmpeg_s"])

            else:
                t_probe = time.time()
                reencode = needs_reencode(str(src))
                timings["ffprobe_s"] = round(time.time() - t_probe, 2)
                log.info("[%s] ffprobe: %.2fs - reencode necesario: %s", job_id[:8], timings["ffprobe_s"], reencode)

                if reencode:
                    cmd_ff = [
                        "ffmpeg", "-y",
                        "-i", str(src),
                        "-vcodec", "libx264",
                        "-acodec", "aac",
                        "-pix_fmt", "yuv420p",
                        "-movflags", "+faststart",
                        "-preset", "ultrafast",
                        "-crf", "23",
                        "-threads", "2",
                        str(dst),
                    ]
                else:
                    cmd_ff = [
                        "ffmpeg", "-y",
                        "-i", str(src),
                        "-c", "copy",
                        "-movflags", "+faststart",
                        str(dst),
                    ]

                proc2 = await asyncio.create_subprocess_exec(
                    *cmd_ff,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                try:
                    stdout2, stderr2 = await asyncio.wait_for(
                        proc2.communicate(), timeout=job.timeout_ff
                    )
                except asyncio.TimeoutError:
                    proc2.kill()
                    await proc2.communicate()
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"] = f"Timeout: encoding supero {job.timeout_ff}s"
                    jobs[job_id]["finished_at"] = time.time()
                    src.unlink(missing_ok=True)
                    return

                if proc2.returncode != 0:
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"] = stderr2.decode(errors="ignore")[-1000:]
                    jobs[job_id]["finished_at"] = time.time()
                    src.unlink(missing_ok=True)
                    return

                src.unlink(missing_ok=True)
                timings["ffmpeg_s"] = round(time.time() - t1, 2)
                log.info("[%s] ffmpeg: %.2fs", job_id[:8], timings["ffmpeg_s"])

            timings["total_s"] = round(time.time() - t_total, 2)
            log.info(
                "[%s] Job completado en %.2fs total | dl=%.2fs probe=%.2fs ff=%.2fs",
                job_id[:8],
                timings["total_s"],
                timings.get("download_s", 0),
                timings.get("ffprobe_s", 0),
                timings.get("ffmpeg_s", 0),
            )

            jobs[job_id]["status"] = "finished"
            jobs[job_id]["filename"] = final_name
            jobs[job_id]["download_url"] = f"{BASE_URL}/files/{final_name}"
            jobs[job_id]["finished_at"] = time.time()
            jobs[job_id]["timings"] = timings

        except Exception as e:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)
            jobs[job_id]["finished_at"] = time.time()
            log.exception("[%s] Error inesperado: %s", job_id[:8], e)