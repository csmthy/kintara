FROM python:3.12-slim

WORKDIR /app

# deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# app code + committed assets (the embedded frontend, world map, realm PNGs)
COPY kintara_tracker.py world_map.jpg ./
COPY MapImages/ ./MapImages/

# Single long-running process: it serves the dashboard AND runs the background
# pollers that build the DB. NOT gunicorn — the workers are threads started in
# main(), and forking would multiply the pollers. One process is correct here.
#
# Runtime knobs (env, all optional — defaults are 24/7-friendly):
#   KINTARA_DB        where the SQLite file lives  (point at a mounted volume!)
#   PORT              listen port (most hosts inject this)
#   POLL_INTERVAL     listing poll seconds (default 90)
#   KINTARA_MIN_GAP   global min seconds between kintara.gg requests (default 0.5)
#   STATS_STALE_HOT / STATS_STALE_COLD  per-item stats refresh cadence (120 / 900)
ENV KINTARA_DB=/data/kintara.db \
    KINTARA_HOST=0.0.0.0 \
    PORT=8765 \
    PYTHONUNBUFFERED=1

# the persistent volume gets mounted here; the DB + icon cache live on it
RUN mkdir -p /data
VOLUME ["/data"]
WORKDIR /data
EXPOSE 8765

CMD ["python", "/app/kintara_tracker.py", "--no-browser"]
