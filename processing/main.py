import os
import json
import psycopg2
import numpy as np
from collections import defaultdict
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

# --- DB connection ---
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        dbname=os.getenv("DB_NAME"),
        connect_timeout=5
    )

# --- Anomaly detection ---
# Keeps a rolling history of cash_amount per ticker in memory.
# Once we have >= MIN_SAMPLES for a ticker, we compute a z-score.
# A z-score beyond THRESHOLD is flagged as anomalous.

MIN_SAMPLES = 5       # minimum history needed before scoring
Z_THRESHOLD = 2.0     # standard deviations before flagging

history = defaultdict(list)  # ticker -> [cash_amount, ...]

def compute_anomaly_score(ticker: str, cash_amount: float):
    """
    Returns (z_score, is_anomaly).
    Returns (None, False) if not enough history yet.
    """
    hist = history[ticker]

    if len(hist) < MIN_SAMPLES:
        history[ticker].append(cash_amount)
        return None, False

    mean = np.mean(hist)
    std  = np.std(hist)

    if std == 0:
        history[ticker].append(cash_amount)
        return 0.0, False

    z_score = abs((cash_amount - mean) / std)
    is_anomaly = z_score > Z_THRESHOLD

    # Add to history after scoring (rolling window, keep last 50)
    history[ticker].append(cash_amount)
    if len(history[ticker]) > 50:
        history[ticker].pop(0)

    return round(z_score, 4), is_anomaly

# --- Cleaning ---
def clean_dividend(dividend: dict):
    """
    Validates and normalises a raw dividend record.
    Returns None if the record should be skipped.
    """
    try:
        cash_amount = float(dividend.get("cash_amount") or 0)
    except (TypeError, ValueError):
        print(f"  [skip] bad cash_amount: {dividend.get('cash_amount')}")
        return None

    if cash_amount <= 0:
        print(f"  [skip] non-positive cash_amount for {dividend.get('ticker')}")
        return None

    return {
        "ticker":           str(dividend.get("ticker", "")).upper().strip(),
        "cash_amount":      cash_amount,
        "currency":         str(dividend.get("currency", "USD")).upper().strip(),
        "declaration_date": dividend.get("declaration_date") or None,
        "dividend_type":    dividend.get("dividend_type") or None,
        "ex_dividend_date": dividend.get("ex_dividend_date") or None,
        "frequency":        dividend.get("frequency") or None,
        "pay_date":         dividend.get("pay_date") or None,
        "record_date":      dividend.get("record_date") or None,
    }

# --- DB insert ---
INSERT_SQL = """
    INSERT INTO predictions (
        ticker, cash_amount, currency,
        declaration_date, dividend_type, ex_dividend_date,
        frequency, pay_date, record_date,
        prediction, is_anomaly, z_score
    ) VALUES (
        %(ticker)s, %(cash_amount)s, %(currency)s,
        %(declaration_date)s, %(dividend_type)s, %(ex_dividend_date)s,
        %(frequency)s, %(pay_date)s, %(record_date)s,
        %(prediction)s, %(is_anomaly)s, %(z_score)s
    )
"""

def insert_record(conn, record: dict):
    with conn.cursor() as cur:
        cur.execute(INSERT_SQL, record)
    conn.commit()

# --- Main loop ---
def main():
    print("Processing service started...")

    kafka_broker = os.getenv("KAFKA_BROKER")
    consumer_config = {
        "bootstrap.servers": kafka_broker,
        "group.id": "processing-group",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(["dividends"])
    print("Listening on 'dividends' topic...")

    conn = get_db_connection()
    print("Connected to PostgreSQL.")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            ticker   = msg.key().decode("utf-8")
            raw      = json.loads(msg.value().decode("utf-8"))
            print(f"\nReceived: {ticker}")

            # 1. Clean
            cleaned = clean_dividend(raw)
            if cleaned is None:
                continue

            # 2. Anomaly detection
            z_score, is_anomaly = compute_anomaly_score(ticker, cleaned["cash_amount"])

            if z_score is None:
                print(f"  [{ticker}] Not enough history yet — storing without score")
            elif is_anomaly:
                print(f"  [{ticker}] ANOMALY detected! z={z_score} cash={cleaned['cash_amount']}")
            else:
                print(f"  [{ticker}] Normal. z={z_score} cash={cleaned['cash_amount']}")

            # 3. Insert into DB
            # prediction column holds the z_score as the model output for now
            record = {
                **cleaned,
                "prediction": float(z_score) if z_score is not None else None,
                "is_anomaly": bool(is_anomaly),
                "z_score":    float(z_score) if z_score is not None else None,
            }
            insert_record(conn, record)
            print(f"  [{ticker}] Inserted into DB.")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()