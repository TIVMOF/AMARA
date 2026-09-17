# amara-gather - crawls 50 storefronts into one JSON per retailer.
#
#   docker run --rm -v amara_gathered:/data/gathered \
#     -e AMARA_SNOWFLAKE_TOKEN=... amara-gather crawl
FROM python:3.14-slim

# print() to a pipe block-buffers at 8 KB, and a container's stdout is a pipe.
# Without this a crawl's progress arrives in lumps and the tail is lost on a
# hard kill, while the logging handler - which flushes per record, onto the
# same fd - interleaves out of order.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gather.py .
COPY scripts/ scripts/
# 50 retailer configs. Versioned config that ships beside the code, not data:
# registry.py finds them relative to this file, so nothing external supplies
# them and changing one means rebuilding this image.
COPY sites/ sites/

# Where this image writes, inside its own filesystem. This is NOT a volume
# declaration - what backs /data/gathered is decided at run time by -v, and
# this code neither knows nor cares whether that is a named volume, a bind
# mount, or the container's own writable layer.
ENV AMARA_GATHERED_DIR=/data/gathered

# Crawler tuning is not secret, so the image carries working defaults and a
# bare `docker run amara-gather crawl` works. The "no code fallbacks" rule from
# CLAUDE.md still holds: these are read from the environment, and there is no
# default inside the code. `docker run -e` overrides any of them, and with no
# .env in the image nothing competes with what is passed in.
ENV AMARA_INGESTION_USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36" \
    AMARA_INGESTION_TIMEOUT=30 \
    AMARA_INGESTION_MAX_RETRIES=3 \
    AMARA_INGESTION_RATE_LIMIT_RPS=1

# Snowflake credentials are deliberately absent: `upload` needs six
# AMARA_SNOWFLAKE_* variables and they arrive at run time.

# A fresh named volume inherits the ownership of the image directory it is
# mounted over, which is what lets this non-root user write to it.
RUN useradd --create-home --uid 1000 amara \
 && mkdir -p /data/gathered \
 && chown -R amara:amara /data
USER amara

ENTRYPOINT ["python", "gather.py"]
CMD ["--help"]
