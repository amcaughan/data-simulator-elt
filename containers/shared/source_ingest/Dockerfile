FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN useradd --create-home --uid 10001 appuser

WORKDIR /app

COPY source_ingest/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY common /app/common
COPY source_ingest /app/source_ingest

RUN chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "-m", "source_ingest"]
