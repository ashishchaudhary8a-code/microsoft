FROM python:3.11-slim

WORKDIR /app
COPY ml /app/ml
COPY streaming /app/streaming
COPY data /app/data
COPY streaming/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8001 8002
CMD ["python", "-m", "streaming.app.main"]
