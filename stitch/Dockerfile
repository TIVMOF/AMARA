# amara-stitch - builds the 12 parquet tables. The only image needing a JVM.
#
#   docker run --rm -v amara_sheared:/data/sheared:ro \
#     -v amara_stitched:/data/stitched amara-stitch
FROM python:3.14-slim

# Spark 4 wants Java 17 or 21; this project runs on 21. JAVA_HOME is left unset
# deliberately - its path is architecture-dependent, and PySpark falls back to
# the `java` on PATH, which apt puts at /usr/bin/java on both amd64 and arm64.
# procps is for Spark's own memory probing.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-21-jre-headless \
      procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stitch.py .
COPY scripts/ scripts/
# The vocabularies - 347 brands, 24 categories and the rest. Versioned config
# that ships beside the code: paths.REFERENCE_ROOT finds them relative to this
# file, so changing a brand means rebuilding this image.
COPY reference/ reference/

# Directories inside this image's own filesystem, not volume declarations -
# see gather/Dockerfile. /data/sheared is read, /data/stitched is written.
ENV AMARA_SHEARED_DIR=/data/sheared \
    AMARA_STITCHED_DIR=/data/stitched

# stitch.py sets no driver memory on purpose: a builder option overrides what
# the submitter asked for, so a hardcoded one would silently turn a cluster
# submission into a local JVM. That leaves PySpark's 1 GB default, which will
# not hold 274K products and 2.7M variants - measured, unset gives a 1.00 GiB
# heap and this gives 8.00 GiB. PySpark reads it when it launches the JVM, so
# it works without spark-submit and `docker run -e` still overrides it.
#
# The master stays PySpark's own local[*]. SPARK_MASTER is deliberately NOT set
# here: it is ignored on this path (measured - the session still came up
# local[*]), and an env var that silently does nothing is worse than none. A
# cluster submission means overriding the entrypoint with spark-submit.
ENV SPARK_DRIVER_MEMORY=8g

RUN useradd --create-home --uid 1000 amara \
 && mkdir -p /data/sheared /data/stitched \
 && chown -R amara:amara /data
USER amara

ENTRYPOINT ["python", "stitch.py"]
CMD []
