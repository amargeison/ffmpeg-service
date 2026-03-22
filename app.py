import sys
import os

print(f"[STARTUP] Python {sys.version}", flush=True)
print(f"[STARTUP] PORT = {os.environ.get('PORT', 'NOT SET')}", flush=True)
print(f"[STARTUP] Importing Flask...", flush=True)

try:
    from flask import Flask, jsonify
    print("[STARTUP] Flask imported OK", flush=True)
except Exception as e:
    print(f"[STARTUP] Flask import FAILED: {e}", flush=True)
    sys.exit(1)

print("[STARTUP] Creating Flask app...", flush=True)
app = Flask(__name__)
print("[STARTUP] Flask app created OK", flush=True)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"[STARTUP] Binding to 0.0.0.0:{port}", flush=True)
    try:
        app.run(host="0.0.0.0", port=port)
    except Exception as e:
        print(f"[STARTUP] Flask failed to start: {e}", flush=True)
        sys.exit(1)
