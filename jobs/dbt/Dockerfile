FROM python:3.12-slim

WORKDIR /app

COPY dbt/requirements.txt /tmp/requirements.txt

RUN if [ -s /tmp/requirements.txt ]; then python -m pip install --no-cache-dir -r /tmp/requirements.txt; fi

COPY dbt /app/dbt

ENTRYPOINT ["python", "-c", "print('dbt runtime placeholder')"]
