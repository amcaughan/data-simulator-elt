FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN if [ -s /tmp/requirements.txt ]; then python -m pip install --no-cache-dir -r /tmp/requirements.txt; fi

COPY . /app/dbt

ENTRYPOINT ["python", "-c", "print('batch-file-delivery dbt placeholder')"]
