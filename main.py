import os
from flask import Flask, jsonify, request
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()

# ✅ Your BigQuery project & dataset
BQ_PROJECT = os.environ.get("BQ_PROJECT", "forestscapemis")
BQ_DATASET = os.environ.get("BQ_DATASET", "forestscape_mis")

# ✅ Simple API key protection (recommended)
API_KEY = os.environ.get("API_KEY", "")  # set in Cloud Run env vars


def require_api_key() -> bool:
    """Returns True if request is authorized."""
    if not API_KEY:
        return True  # testing mode if you didn't set API_KEY
    auth = request.headers.get("Authorization", "")
    return auth == f"Bearer {API_KEY}"


@app.before_request
def auth_guard():
    # Allow health check without key
    if request.path == "/health":
        return None

    if not require_api_key():
        return jsonify({"error": "Unauthorized"}), 401

    return None


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/")
def root():
    return jsonify(
        {
            "service": "forestscape-api",
            "endpoints": ["/health", "/monthwise", "/unit-summary", "/unit/<unit_no>"],
        }
    )


@app.get("/monthwise")
def monthwise():
    sql = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.monthwise_collection`
        ORDER BY month
    """
    rows = bq.query(sql).result()
    return jsonify([dict(r) for r in rows])


@app.get("/unit-summary")
def unit_summary():
    sql = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.unit_summary`
    """
    rows = bq.query(sql).result()
    return jsonify([dict(r) for r in rows])


@app.get("/unit/<unit_no>")
def unit(unit_no: str):
    sql = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.unit_summary`
        WHERE UNIT_NO = @unit_no
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("unit_no", "STRING", unit_no)]
    )
    rows = list(bq.query(sql, job_config=job_config).result())
    return jsonify(dict(rows[0]) if rows else {})


if __name__ == "__main__":
    # Local run (Cloud Run uses gunicorn)
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
