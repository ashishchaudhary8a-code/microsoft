FROM python:3.11-slim
WORKDIR /app
COPY data /app/data
COPY data/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "data.generators.generate", "--scenario", "normal", "--rate", "4"]
