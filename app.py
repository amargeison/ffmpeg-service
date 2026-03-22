import os
import uuid
import json
import shutil
import subprocess
import threading
import time
from functools import wraps
from pathlib import Path

import requests
from flask import Flask, jsonify, send_from_directory, request, abort

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WORK_DIR = Path(os.environ.get("WORK_DIR", "/tmp/ffmpeg-service"))
WORK_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = os.environ.get("BASE_URL", "http://localhost:5000").rstrip("/")
API_KEY = os.environ.get("API_KEY")          # Optional — if set, POSTs require X-API-Key header
CLEANUP_AFTER_SECONDS = int(os.environ.get("CLEANUP_AFTER_SECONDS", 7200))  # 2h


# ---------------------------------------------------------------------------
# Background cleanup — delete output .mp4 files older than CLEANUP_AFTER_SECONDS
# ---------------------------------------------------------------------------
def _cleanup_loop():
    while True:
        time.sleep(600)  # run every 10 min
        now = time.time()
        for f in WORK_DIR.glob("*.mp4"):
            try:
                if now - f.stat().st_mtime > CLEANUP_AFTER_SECONDS:
                    f.unlink(missing_ok=True)
            except OSError:
                pass

threading.Thread(target=_cleanup_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Auth decorator
# ---------------------------------------------------------------------------
def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if API_KEY:
            key = request.headers.get("X-API-Key") or request.args.get("api_key")
            if key != API_KEY:
                return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def is_youtube_url(url: str) -> bool:
    return "youtube.com" in url or "youtu.be" in url


def download_video(url: str, dest_dir: Path) -> Path:
    """Download video to dest_dir. Uses yt-dlp for YouTube, requests for direct URLs."""
    if is_youtube_url(url):
        result = subprocess.run(
            [
                "yt-dlp",
                "-f", "bestvideo[ext=mp4][height<=1080]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "--merge-output-format", "mp4",
                "-o", str(dest_dir / "input.%(ext)s"),
                url,
            ],
            capture_output=True, text=True, timeout=300,
        )
        if result.returncode != 0:
            raise RuntimeError(f"yt-dlp failed: {result.stderr[-1000:]}")
        files = list(dest_dir.glob("input.*"))
        if not files:
            raise RuntimeError("yt-dlp produced no output file")
        return files[0]
    else:
        dest = dest_dir / "input.mp4"
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                fh.write(chunk)
        return dest


def run_ffmpeg(args: list, timeout: int = 600):
    """Run ffmpeg -y <args>. Returns (returncode, stderr)."""
    result = subprocess.run(
        ["ffmpeg", "-y"] + args,
        capture_output=True, text=True, timeout=timeout,
    )
    return result.returncode, result.stderr


def get_duration(path: Path):
    """Return video duration in seconds (float) via ffprobe, or None on failure."""
    result = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(path)],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        try:
            return float(json.loads(result.stdout).get("format", {}).get("duration", 0))
        except (ValueError, KeyError, json.JSONDecodeError):
            pass
    return None


def safe_filename(name: str) -> str:
    """Strip path components to prevent directory traversal."""
    return Path(name).name


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    result = subprocess.run(["ffmpeg", "-version"], capture_output=True)
    return jsonify({
        "status": "ok",
        "ffmpeg": "available" if result.returncode == 0 else "unavailable",
    })


@app.route("/extract", methods=["POST"])
@require_auth
def extract():
    """
    Extract a clip from a video.

    Request JSON:
        url            (str, required)  — direct MP4 URL or YouTube URL
        start          (str, optional)  — start timestamp, e.g. "00:01:30" (default: "00:00:00")
        end            (str, required)  — end timestamp, e.g. "00:02:00"
        output_filename (str, optional) — desired filename (default: clip_<random>.mp4)

    Response JSON:
        success        (bool)
        output_url     (str)  — URL to download the output file
        filename       (str)
        duration_seconds (float|null)
    """
    data = request.get_json(force=True, silent=True) or {}
    url = data.get("url")
    start = data.get("start", "00:00:00")
    end = data.get("end")
    filename = safe_filename(data.get("output_filename") or f"clip_{uuid.uuid4().hex[:8]}.mp4")

    if not url:
        return jsonify({"error": "'url' is required"}), 400
    if not end:
        return jsonify({"error": "'end' timestamp is required"}), 400

    job_dir = WORK_DIR / uuid.uuid4().hex
    job_dir.mkdir()
    output_path = WORK_DIR / filename

    try:
        input_path = download_video(url, job_dir)

        rc, stderr = run_ffmpeg([
            "-i", str(input_path),
            "-ss", start,
            "-to", end,
            "-c:v", "libx264",
            "-c:a", "aac",
            "-preset", "fast",
            str(output_path),
        ])

        if rc != 0:
            return jsonify({"error": "FFmpeg failed", "details": stderr[-2000:]}), 500

        return jsonify({
            "success": True,
            "output_url": f"{BASE_URL}/files/{filename}",
            "filename": filename,
            "duration_seconds": get_duration(output_path),
        })

    except subprocess.TimeoutExpired:
        return jsonify({"error": "Operation timed out"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


@app.route("/compile", methods=["POST"])
@require_auth
def compile_clips():
    """
    Download clips, trim each one, then concatenate into a single video.

    Request JSON:
        clips  (list, required) — array of:
            url    (str, required)  — direct MP4 URL or YouTube URL
            start  (str, optional)  — start timestamp (default: "00:00:00")
            end    (str, optional)  — end timestamp (omit to keep to end of file)
        output_filename (str, optional) — desired filename (default: compilation_<random>.mp4)

    Response JSON:
        success          (bool)
        output_url       (str)
        filename         (str)
        clip_count       (int)
        duration_seconds (float|null)
    """
    data = request.get_json(force=True, silent=True) or {}
    clips = data.get("clips", [])
    filename = safe_filename(
        data.get("output_filename") or f"compilation_{uuid.uuid4().hex[:8]}.mp4"
    )

    if not clips:
        return jsonify({"error": "'clips' array is required and must not be empty"}), 400

    job_dir = WORK_DIR / uuid.uuid4().hex
    job_dir.mkdir()
    output_path = WORK_DIR / filename
    concat_list = job_dir / "concat.txt"

    try:
        clip_paths = []

        for i, clip in enumerate(clips):
            url = clip.get("url")
            start = clip.get("start", "00:00:00")
            end = clip.get("end")

            if not url:
                return jsonify({"error": f"clips[{i}] is missing 'url'"}), 400

            clip_dir = job_dir / str(i)
            clip_dir.mkdir()

            raw = download_video(url, clip_dir)
            clipped = job_dir / f"clip_{i}.mp4"

            args = ["-i", str(raw), "-ss", start]
            if end:
                args += ["-to", end]
            args += ["-c:v", "libx264", "-c:a", "aac", "-preset", "fast", str(clipped)]

            rc, stderr = run_ffmpeg(args)
            if rc != 0:
                return jsonify({
                    "error": f"FFmpeg failed on clip {i}",
                    "details": stderr[-2000:],
                }), 500

            clip_paths.append(clipped)

        # Write ffmpeg concat file
        with open(concat_list, "w") as fh:
            for cp in clip_paths:
                fh.write(f"file '{cp}'\n")

        # Concatenate — stream copy since all clips are already H.264/AAC
        rc, stderr = run_ffmpeg([
            "-f", "concat",
            "-safe", "0",
            "-i", str(concat_list),
            "-c", "copy",
            str(output_path),
        ])
        if rc != 0:
            return jsonify({"error": "FFmpeg concat failed", "details": stderr[-2000:]}), 500

        return jsonify({
            "success": True,
            "output_url": f"{BASE_URL}/files/{filename}",
            "filename": filename,
            "clip_count": len(clips),
            "duration_seconds": get_duration(output_path),
        })

    except subprocess.TimeoutExpired:
        return jsonify({"error": "Operation timed out"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        shutil.rmtree(job_dir, ignore_errors=True)


@app.route("/files/<path:filename>")
def serve_file(filename):
    """Serve a previously generated .mp4 output file."""
    safe = safe_filename(filename)
    if not safe.endswith(".mp4"):
        abort(404)
    path = WORK_DIR / safe
    if not path.exists():
        abort(404)
    return send_from_directory(str(WORK_DIR), safe, as_attachment=False)


# ---------------------------------------------------------------------------
# Entry point (local dev only — production uses gunicorn)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
