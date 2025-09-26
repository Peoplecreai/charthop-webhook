FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# código
COPY app/ app/
COPY handlers/ handlers/

# para imports relativos desde /app
ENV PYTHONPATH=/app

# gunicorn según tu estructura: app/main.py expone "app"
CMD ["gunicorn", "-b", ":8080", "app.main:app"]
