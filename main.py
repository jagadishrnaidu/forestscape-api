import os
from datetime import datetime
from flask import Flask, jsonify, request
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()

BQ_PROJECT = os.getenv("BQ_PROJECT", "")
BQ_DATASET = os.getenv("BQ_DATASET", "")
BQ_TABLE = os.getenv("BQ_TABLE", "soldmis")  # set this env var if your table name differs
API_KEY = os.getenv("API_KEY", "")

def require_bearer_auth():
    # Expect: Authorization: Bearer <API_KEY>
    if not API_KEY:
        return None  # allow if no key is configured (optional)
    auth = request.headers.get("Authorization", "")
    if auth.strip() != f"Bearer {API_KEY}":
        return jsonify({"error": "Unauthorized"}), 401
    return None

@app.before_request
def auth_guard():
    # Allow health without auth (optional)
    if request.path in ("/health", "/routes"):
        return None
    return require_bearer_auth()

def table_fqn():
    if not (BQ_PROJECT and BQ_DATASET and BQ_TABLE):
        raise ValueError("Missing BQ_PROJECT / BQ_DATASET / BQ_TABLE env vars")
    return f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"

@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "Forestscape MIS", "ts": datetime.utcnow().isoformat()})

@app.get("/routes")
def routes():
    return {"routes": sorted([str(r) for r in app.url_map.iter_rules()])}

def get_date_range():
    frm = request.args.get("from")
    to = request.args.get("to")
    if not frm or not to:
        return None, None, (jsonify({"error": "Missing required query params: from, to (YYYY-MM-DD)"}), 400)
    return frm, to, None

@app.get("/soldmis/summary")
def soldmis_summary():
    frm, to, err = get_date_range()
    if err:
        return err

    # NOTE: Update column names if needed.
    # Assuming your BigQuery table has columns:
    # DATE (date), GROSS_AMOUNT_RECEIVED (numeric), PENDING_DEMAND (numeric)
    sql = f"""
      SELECT
        SUM(CAST(GROSS_AMOUNT_RECEIVED AS NUMERIC)) AS gross_amount_received,
        SUM(CAST(PENDING_DEMAND AS NUMERIC)) AS pending_demand,
        COUNT(1) AS rows
      FROM {table_fqn()}
      WHERE DATE BETWEEN @from AND @to
    """

    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("from", "DATE", frm),
                bigquery.ScalarQueryParameter("to", "DATE", to),
            ]
        ),
    )
    row = next(iter(job.result()), None)
    return jsonify({
        "from": frm,
        "to": to,
        "gross_amount_received": float(row["gross_amount_received"] or 0),
        "pending_demand": float(row["pending_demand"] or 0),
        "rows": int(row["rows"] or 0),
    })

@app.get("/soldmis/breakdown")
def soldmis_breakdown():
    frm, to, err = get_date_range()
    if err:
        return err

    # Breakdown by SALE_AGREEMENT_STATUS
    sql = f"""
      SELECT
        COALESCE(SALE_AGREEMENT_STATUS, "UNKNOWN") AS sale_agreement_status,
        SUM(CAST(GROSS_AMOUNT_RECEIVED AS NUMERIC)) AS gross_amount_received,
        SUM(CAST(PENDING_DEMAND AS NUMERIC)) AS pending_demand,
        COUNT(1) AS rows
      FROM {table_fqn()}
      WHERE DATE BETWEEN @from AND @to
      GROUP BY sale_agreement_status
      ORDER BY gross_amount_received DESC
    """

    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("from", "DATE", frm),
                bigquery.ScalarQueryParameter("to", "DATE", to),
            ]
        ),
    )
    data = []
    for r in job.result():
        data.append({
            "sale_agreement_status": r["sale_agreement_status"],
            "gross_amount_received": float(r["gross_amount_received"] or 0),
            "pending_demand": float(r["pending_demand"] or 0),
            "rows": int(r["rows"] or 0),
        })

    return jsonify({"from": frm, "to": to, "data": data})

@app.get("/soldmis/unit")
def soldmis_unit():
    # Example: /soldmis/unit?unit_no=...
    unit_no = request.args.get("unit_no")
    if not unit_no:
        return jsonify({"error": "Missing query param: unit_no"}), 400

    sql = f"""
      SELECT *
      FROM {table_fqn()}
      WHERE UNIT_NO = @unit_no
      LIMIT 50
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("unit_no", "STRING", unit_no)]
        ),
    )
    rows = [dict(r) for r in job.result()]
    return jsonify({"unit_no": unit_no, "rows": rows})

@app.get("/soldmis/payments")
def soldmis_payments():
    # Returns payment columns for a unit (PAYMENT_1..PAYMENT_20) + totals
    unit_no = request.args.get("unit_no")
    if not unit_no:
        return jsonify({"error": "Missing query param: unit_no"}), 400

    sql = f"""
      SELECT
        UNIT_NO,
        PAYMENT_1, PAYMENT_2, PAYMENT_3, PAYMENT_4, PAYMENT_5,
        PAYMENT_6, PAYMENT_7, PAYMENT_8, PAYMENT_9, PAYMENT_10,
        PAYMENT_11, PAYMENT_12, PAYMENT_13, PAYMENT_14, PAYMENT_15,
        PAYMENT_16, PAYMENT_17, PAYMENT_18, PAYMENT_19, PAYMENT_20,
        GROSS_AMOUNT_RECEIVED,
        PENDING_DEMAND,
        SALE_AGREEMENT_STATUS
      FROM {table_fqn()}
      WHERE UNIT_NO = @unit_no
      LIMIT 1
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("unit_no", "STRING", unit_no)]
        ),
    )
    r = next(iter(job.result()), None)
    if not r:
        return jsonify({"error": "Unit not found"}), 404
    return jsonify(dict(r))
