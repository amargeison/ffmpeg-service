import os
import sys
import uuid
import json
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

print(f"[STARTUP] Python {sys.version}", flush=True)
print(f"[STARTUP] PORT = {os.environ.get('PORT', 'NOT SET')}", flush=True)

import requests
from flask import Flask, jsonify, send_from_directory, request, abort

print("[STARTUP] All imports OK", flush=True)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WORK_DIR = Path(os.environ.get("WORK_DIR", "/tmp/ffmpeg-service"))
WORK_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = os.environ.get("BASE_URL", "").rstrip("/")
API_KEY = os.environ.get("API_KEY")
CLEANUP_AFTER_SECONDS = int(os.environ.get("CLEANUP_AFTER_SECONDS", 7200))

RTMP_BASE = "rtmp://a.rtmp.youtube.com/live2"
MAX_RESTARTS = 5
RESTART_DELAY = 30  # seconds between auto-restart attempts


# ---------------------------------------------------------------------------
# Stream Manager
# ---------------------------------------------------------------------------
_streams = {}        # streamId -> info dict
_streams_lock = threading.Lock()


def _resolve_stream_url(source_url: str) -> str:
    """Use yt-dlp -g to resolve a direct playable stream URL."""
    result = subprocess.run(
        ["yt-dlp", "-g", "--no-playlist", source_url],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp could not resolve URL: {result.stderr[-500:]}")
    url = result.stdout.strip().split("\n")[0]
    if not url:
        raise RuntimeError("yt-dlp returned an empty URL")
    return url


def _start_ffmpeg_process(resolved_url: str, stream_key: str) -> subprocess.Popen:
    """Spawn a non-blocking FFmpeg re-stream process."""
    rtmp_dest = f"{RTMP_BASE}/{stream_key}"
    cmd = [
        "ffmpeg", "-re",
        "-i", resolved_url,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-b:v", "3000k",
        "-maxrate", "3000k",
        "-bufsize", "6000k",
        "-pix_fmt", "yuv420p",
        "-g", "60",
        "-c:a", "aac",
        "-b:a", "128k",
        "-ar", "44100",
        "-f", "flv",
        rtmp_dest,
    ]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _watcher(stream_id: str):
    """
    Background thread: watches for unexpected FFmpeg exit and auto-restarts
    up to MAX_RESTARTS times, with RESTART_DELAY seconds between attempts.
    """
    while True:
        with _streams_lock:
            info = _streams.get(stream_id)
        if info is None or info.get("stop_requested"):
            return

        proc = info.get("process")
        if proc is None:
            return

        ret = proc.wait()  # blocks until this process exits

        with _streams_lock:
            info = _streams.get(stream_id)
            if info is None or info.get("stop_requested"):
                return
            restart_count = info.get("restartCount", 0)

        if restart_count >= MAX_RESTARTS:
            print(f"[STREAM] {stream_id} hit max restarts ({MAX_RESTARTS}). Removing.", flush=True)
            with _streams_lock:
                _streams.pop(stream_id, None)
            return

        print(
            f"[STREAM] {stream_id} exited (code {ret}). "
            f"Restart {restart_count + 1}/{MAX_RESTARTS} in {RESTART_DELAY}s…",
            flush=True,
        )
        time.sleep(RESTART_DELAY)

        with _streams_lock:
            info = _streams.get(stream_id)
            if info is None or info.get("stop_requested"):
                return

        try:
            resolved = _resolve_stream_url(info["sourceUrl"])
            new_proc = _start_ffmpeg_process(resolved, info["youtubeStreamKey"])
            with _streams_lock:
                if stream_id in _streams:
                    _streams[stream_id]["process"] = new_proc
                    _streams[stream_id]["pid"] = new_proc.pid
                    _streams[stream_id]["restartCount"] = restart_count + 1
                    _streams[stream_id]["lastRestartAt"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            print(f"[STREAM] {stream_id} restart attempt {restart_count + 1} failed: {exc}", flush=True)
            with _streams_lock:
                if stream_id in _streams:
                    _streams[stream_id]["restartCount"] = restart_count + 1
            time.sleep(RESTART_DELAY)


def _do_start_stream(stream_id: str, source_url: str, stream_key: str, label: str) -> dict:
    """Resolve URL, start FFmpeg, register in manager, launch watcher. Returns info dict."""
    resolved = _resolve_stream_url(source_url)
    proc = _start_ffmpeg_process(resolved, stream_key)
    started_at = datetime.now(timezone.utc).isoformat()

    info = {
        "streamId": stream_id,
        "label": label,
        "sourceUrl": source_url,
        "youtubeStreamKey": stream_key,
        "process": proc,
        "pid": proc.pid,
        "startedAt": started_at,
        "restartCount": 0,
        "lastRestartAt": None,
        "stop_requested": False,
    }

    with _streams_lock:
        _streams[stream_id] = info

    threading.Thread(target=_watcher, args=(stream_id,), daemon=True).start()
    return info


# ---------------------------------------------------------------------------
# Background file cleanup
# ---------------------------------------------------------------------------
def _cleanup_loop():
    while True:
        time.sleep(600)
        now = time.time()
        for f in WORK_DIR.glob("*.mp4"):
            try:
                if now - f.stat().st_mtime > CLEANUP_AFTER_SECONDS:
                    f.unlink(missing_ok=True)
            except OSError:
                pass

threading.Thread(target=_cleanup_loop, daemon=True).start()
print("[STARTUP] Cleanup thread started", flush=True)


# ---------------------------------------------------------------------------
# Auth
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
# Helpers (clip/compile)
# ---------------------------------------------------------------------------
def is_youtube_url(url: str) -> bool:
    return "youtube.com" in url or "youtu.be" in url


def download_video(url: str, dest_dir: Path) -> Path:
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
    result = subprocess.run(
        ["ffmpeg", "-y"] + args,
        capture_output=True, text=True, timeout=timeout,
    )
    return result.returncode, result.stderr


def get_duration(path: Path):
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
    return Path(name).name


# ---------------------------------------------------------------------------
# Routes — core
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    result = subprocess.run(["ffmpeg", "-version"], capture_output=True)
    with _streams_lock:
        active_count = len(_streams)
    return jsonify({
        "status": "ok",
        "ffmpeg": "available" if result.returncode == 0 else "unavailable",
        "activeStreams": active_count,
    })


@app.route("/extract", methods=["POST"])
@require_auth
def extract():
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

        with open(concat_list, "w") as fh:
            for cp in clip_paths:
                fh.write(f"file '{cp}'\n")

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
    safe = safe_filename(filename)
    if not safe.endswith(".mp4"):
        abort(404)
    path = WORK_DIR / safe
    if not path.exists():
        abort(404)
    return send_from_directory(str(WORK_DIR), safe, as_attachment=False)


# ---------------------------------------------------------------------------
# Routes — stream management
# ---------------------------------------------------------------------------

@app.route("/stream/start", methods=["POST"])
@require_auth
def stream_start():
    data = request.get_json(force=True, silent=True) or {}
    stream_id = data.get("streamId")
    source_url = data.get("sourceUrl")
    stream_key = data.get("youtubeStreamKey")
    label = data.get("label", stream_id)

    if not stream_id or not source_url or not stream_key:
        return jsonify({"error": "streamId, sourceUrl, and youtubeStreamKey are required"}), 400

    with _streams_lock:
        if stream_id in _streams:
            return jsonify({"error": f"Stream '{stream_id}' is already running"}), 409

    try:
        info = _do_start_stream(stream_id, source_url, stream_key, label)
        return jsonify({
            "success": True,
            "streamId": stream_id,
            "pid": info["pid"],
            "startedAt": info["startedAt"],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/stream/stop", methods=["POST"])
@require_auth
def stream_stop():
    data = request.get_json(force=True, silent=True) or {}
    stream_id = data.get("streamId")

    if not stream_id:
        return jsonify({"error": "streamId is required"}), 400

    with _streams_lock:
        info = _streams.get(stream_id)
        if not info:
            return jsonify({"error": f"No active stream '{stream_id}'"}), 404
        info["stop_requested"] = True
        proc = info.get("process")

    if proc:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    with _streams_lock:
        _streams.pop(stream_id, None)

    return jsonify({
        "success": True,
        "streamId": stream_id,
        "stoppedAt": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/stream/status", methods=["GET"])
@require_auth
def stream_status():
    with _streams_lock:
        streams = [
            {
                "streamId": info["streamId"],
                "label": info["label"],
                "pid": info["pid"],
                "startedAt": info["startedAt"],
                "restartCount": info["restartCount"],
                "lastRestartAt": info.get("lastRestartAt"),
                "sourceUrl": info["sourceUrl"],
            }
            for info in _streams.values()
        ]
    return jsonify({"streams": streams})


@app.route("/stream/restart", methods=["POST"])
@require_auth
def stream_restart():
    data = request.get_json(force=True, silent=True) or {}
    stream_id = data.get("streamId")

    if not stream_id:
        return jsonify({"error": "streamId is required"}), 400

    with _streams_lock:
        info = _streams.get(stream_id)
        if not info:
            return jsonify({"error": f"No active stream '{stream_id}'"}), 404
        info["stop_requested"] = True
        proc = info.get("process")
        source_url = info["sourceUrl"]
        stream_key = info["youtubeStreamKey"]
        label = info["label"]
        prev_restart_count = info["restartCount"]

    if proc:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    with _streams_lock:
        _streams.pop(stream_id, None)

    try:
        new_info = _do_start_stream(stream_id, source_url, stream_key, label)
        new_restart_count = prev_restart_count + 1
        with _streams_lock:
            if stream_id in _streams:
                _streams[stream_id]["restartCount"] = new_restart_count
        return jsonify({
            "success": True,
            "streamId": stream_id,
            "restartCount": new_restart_count,
            "pid": new_info["pid"],
            "startedAt": new_info["startedAt"],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/stream/start-all", methods=["POST"])
@require_auth
def stream_start_all():
    data = request.get_json(force=True, silent=True) or {}
    streams = data.get("streams", [])

    if not streams:
        return jsonify({"error": "'streams' array is required and must not be empty"}), 400

    results = []
    for s in streams:
        stream_id = s.get("streamId")
        source_url = s.get("sourceUrl")
        stream_key = s.get("youtubeStreamKey")
        label = s.get("label", stream_id)

        if not stream_id or not source_url or not stream_key:
            results.append({"streamId": stream_id, "success": False, "error": "Missing required fields"})
            continue

        with _streams_lock:
            if stream_id in _streams:
                results.append({"streamId": stream_id, "success": False, "error": "Already running"})
                continue

        try:
            info = _do_start_stream(stream_id, source_url, stream_key, label)
            results.append({"streamId": stream_id, "success": True, "pid": info["pid"], "startedAt": info["startedAt"]})
        except Exception as e:
            results.append({"streamId": stream_id, "success": False, "error": str(e)})

    return jsonify({"results": results})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"[STARTUP] Binding to 0.0.0.0:{port}", flush=True)
    app.run(host="0.0.0.0", port=port)
