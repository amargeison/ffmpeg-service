FROM python:3.11-slim

RUN apt-get update && apt-get install -y ffmpeg curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG CACHEBUST=3

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /tmp/ffmpeg-service

EXPOSE 8080

CMD ["python", "app.py"]
