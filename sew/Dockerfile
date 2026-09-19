# amara-sew - loads the staged parquets into Snowflake and derives the star
# schema. Needs no volume: everything it works on is already in Snowflake.
#
#   docker run --rm --init -e AMARA_SNOWFLAKE_TOKEN=... amara-sew load-processed
FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sew.py .
COPY scripts/ scripts/

# No data directories and no config files: this stage builds exactly one path
# in its whole source, its .env, and that is absent here on purpose. All seven
# AMARA_SNOWFLAKE_* variables arrive at run time.

RUN useradd --create-home --uid 1000 amara
USER amara

ENTRYPOINT ["python", "sew.py"]
CMD ["--help"]
