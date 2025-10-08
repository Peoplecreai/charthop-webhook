FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Puerto por defecto de Flask
ENV PORT=8080
CMD ["sh", "-c", "gunicorn -b :${PORT:-8080} app.main:app"]
