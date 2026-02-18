FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kubernetes==31.0.0 requests==2.32.3

COPY reaper.py .

USER 1000:1000

ENTRYPOINT ["python", "reaper.py"]
