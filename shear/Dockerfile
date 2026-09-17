# amara-shear - cuts each raw crawl into three Spark-shaped files.
#
#   docker run --rm -v amara_gathered:/data/gathered:ro \
#     -v amara_sheared:/data/sheared amara-shear
FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# No pip install: this stage is standard library only, by design. Its
# requirements.txt is a comment block saying so, and is not copied.

COPY shear.py .
COPY scripts/ scripts/

# Directories inside this image's own filesystem, not volume declarations -
# see gather/Dockerfile. /data/gathered is read, /data/sheared is written.
ENV AMARA_GATHERED_DIR=/data/gathered \
    AMARA_SHEARED_DIR=/data/sheared

# This stage reads a whole crawl file into memory at once and the largest is
# ~308 MB, so give the container 4 GB. Peak is set by the biggest single file,
# not by the 2.8 GB total.

RUN useradd --create-home --uid 1000 amara \
 && mkdir -p /data/gathered /data/sheared \
 && chown -R amara:amara /data
USER amara

# No CMD: this stage's default action is the work itself, so a bare
# `docker run amara-shear` shears the current crawl.
ENTRYPOINT ["python", "shear.py"]
