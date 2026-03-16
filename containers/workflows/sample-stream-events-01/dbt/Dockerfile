FROM python:3.12-slim

RUN useradd --create-home --uid 10001 appuser

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /app/dbt

RUN chmod +x /app/dbt/run-dbt.sh \
    && chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["/app/dbt/run-dbt.sh"]
