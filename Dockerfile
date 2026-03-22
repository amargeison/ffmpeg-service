FROM python:3.11-slim

# Install FFmpeg (includes ffprobe) + curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Pre-create the work directory (Railway ephemeral storage)
RUN mkdir -p /tmp/ffmpeg-service

EXPOSE 5000

# gunicorn: 2 workers, 4 threads each, 10-min timeout for long FFmpeg jobs
CMD ["gunicorn", \
     "--bind", "0.0.0.0:5000", \
     "--timeout", "600", \
     "--workers", "2", \
     "--threads", "4", \
     "--worker-class", "gthread", \
     "app:app"]
