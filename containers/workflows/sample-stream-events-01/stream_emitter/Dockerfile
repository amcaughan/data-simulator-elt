FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY app.py /app/app.py

ENTRYPOINT ["python", "/app/app.py"]
