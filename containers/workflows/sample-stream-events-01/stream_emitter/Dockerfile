FROM python:3.12-slim

RUN useradd --create-home --uid 10001 appuser

WORKDIR /app

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

COPY app.py /app/app.py

RUN chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "/app/app.py"]
