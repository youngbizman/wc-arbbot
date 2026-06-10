FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    WC_ARBBOT_MARKETS_PATH=/app/data/markets.json \
    WC_ARBBOT_TAXONOMY_PATH=/app/data/taxonomy.json

WORKDIR /app

RUN addgroup --system app && \
    adduser --system --ingroup app app && \
    mkdir -p /app/data && \
    chown -R app:app /app

COPY requirements.txt ./
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt

COPY indexer.py nlp_mapper.py websocket_signaler.py telegram_bot.py ./

USER app

CMD ["sh", "-c", "python indexer.py && python nlp_mapper.py && python websocket_signaler.py"]
