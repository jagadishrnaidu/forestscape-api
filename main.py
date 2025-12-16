import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional

from flask import Flask, jsonify, request
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()

# ---- ENV ----
BQ_PROJECT = os.getenv("BQ_PROJECT", "")
BQ_DATASET = os.getenv("BQ_DATASET", "")
BQ_TABLE = os.getenv("BQ_TABLE", "soldmis")     # set if table differs
DATE_COL = os.getenv("DATE_COL", "DATE")        # set if your date column differs
API_KEY = os.getenv("API_KEY", "")

# ---- AUTH ----
def require_bearer_auth():
    # Expect: Authorization: Bearer <API_KEY>
    if not API_KEY:
        return None  # allow if no key configured
    auth = request.headers.get("Authorization", "")
    if auth.strip() != f"Bearer {API_KEY}":
        return jsonify({"error": "Unauthorized"}), 401
    return None

@app.before_request
def auth_guard():
    # Allow health/routes without auth
    if request.path in ("/health", "/routes"):
        return None
    return require_bearer_auth()

# ---- HELPERS ----
def table_fqn() -> str:
    if not (BQ_PROJECT and BQ_DATASET and BQ_TABLE):
        raise ValueError("Missing BQ_PROJECT / BQ_DATASET / BQ_TABLE env vars")
    return f"`{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"

def info_schema_fqn() -> str:
    if not (BQ_PROJECT and BQ_DATASET):
        raise ValueError("Missing BQ_PROJECT / BQ_DATASET env vars")
    return f"`{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`"

_COLUMNS_CACHE: Optional[set] = None

def get_columns() -> set:
    """
    Read available columns from INFORMATION_SCHEMA once and cache.
    This prevents BigQuery errors when a column doesn't exist; we can safely fallback to 0/null.
    """
    global _COLUMNS_CACHE
    if _COLUMNS_CACHE is not None:
        return _COLUMNS_CACHE

    sql = f"""
      SELECT UPPER(column_name) AS column_name
      FROM {info_schema_fqn()}
      WHERE table_name = @table_name
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table_name", "STRING", BQ_TABLE)
            ]
        ),
    )
    _COLUMNS_CACHE = {str(r["column_name"]) for r in job.result()}
    return _COLUMNS_CACHE

def has_col(col: str) -> bool:
    return col.upper() in get_columns()

def safe_num_expr(col: str) -> str:
    # Returns a SQL expression that sums numeric-ish columns safely.
    # If missing column: "0"
    if not has_col(col):
        return "0"
    return f"SAFE_CAST({col} AS NUMERIC)"

def safe_str_expr(col: str) -> str:
    if not has_col(col):
        return '""'
    return f"CAST({col} AS STRING)"

def get_date_range() -> Tuple[Optional[str], Optional[str], Optional[Tuple]]:
    frm = request.args.get("from")
    to = request.args.get("to")
    if not frm or not to:
        return None, None, (jsonify({"error": "Missing required query params: from, to (YYYY-MM-DD)"}), 400)
    return frm, to, None

def build_filters_where(params: Dict[str, str]) -> Tuple[str, List[bigquery.ScalarQueryParameter], Dict]:
    """
    Supports optional filters:
      cluster, source, unit_type, sale_agreement_status, loan_status
    Only applies filters if corresponding columns exist in BigQuery.
    """
    where = []
    qp: List[bigquery.ScalarQueryParameter] = []
    filters_echo = {}

    mapping = [
        ("cluster", "CLUSTER"),
        ("source", "SOURCE"),
        ("unit_type", "UNIT_TYPE"),
        ("sale_agreement_status", "SALE_AGREEMENT_STATUS"),
        ("loan_status", "LOAN_STATUS"),
    ]

    for qname, col in mapping:
        val = params.get(qname)
        if val:
            filters_echo[qname] = val
            if has_col(col):
                where.append(f"UPPER(CAST({col} AS STRING)) = UPPER(@{qname})")
                qp.append(bigquery.ScalarQueryParameter(qname, "STRING", val))
            # If column doesn't exist, we ignore the filter to avoid breaking the API.

    if where:
        return " AND " + " AND ".join(where), qp, filters_echo
    return "", qp, filters_echo

# ---- ROUTES ----
@app.get("/health")
def health():
    return jsonify({"status": "ok"})

@app.get("/routes")
def routes():
    return {"routes": sorted([f"{r.rule} [{','.join(sorted(r.methods - {'HEAD','OPTIONS'}))}]"
                              for r in app.url_map.iter_rules()])}

@app.get("/soldmis/summary")
def soldmis_summary():
    frm, to, err = get_date_range()
    if err:
        return err

    extra_where, extra_qp, filters_echo = build_filters_where(request.args)

    # Totals required by schema:
    # bookings, gross_sale_value, sale_value, gross_amount_received, pending_demand, receivables, avg_per_sft_price
    # We'll compute only if columns exist; missing columns become 0/null.

    bookings_expr = "COUNT(1)"  # always valid

    gross_sale_value_expr = f"SUM({safe_num_expr('GROSS_SOLD_SALE_VALUE')})" if has_col("GROSS_SOLD_SALE_VALUE") else "0"
    sale_value_expr = f"SUM({safe_num_expr('SOLD_SALE_VALUE')})" if has_col("SOLD_SALE_VALUE") else (
        f"SUM({safe_num_expr('SALE_VALUE')})" if has_col("SALE_VALUE") else "0"
    )

    gross_amount_received_expr = f"SUM({safe_num_expr('GROSS_AMOUNT_RECEIVED')})" if has_col("GROSS_AMOUNT_RECEIVED") else "0"
    pending_demand_expr = f"SUM({safe_num_expr('PENDING_DEMAND')})" if has_col("PENDING_DEMAND") else "0"
    receivables_expr = f"SUM({safe_num_expr('RECEIVABLES')})" if has_col("RECEIVABLES") else "0"

    # avg_per_sft_price: try PER_SFT_PRICE or APPROVED_PER_SFT_PRICE or compute APPROVED_PRICE/SALEABLE_AREA
    if has_col("PER_SFT_PRICE"):
        avg_per_sft_expr = f"AVG({safe_num_expr('PER_SFT_PRICE')})"
    elif has_col("APPROVED_PER_SFT_PRICE"):
        avg_per_sft_expr = f"AVG({safe_num_expr('APPROVED_PER_SFT_PRICE')})"
    elif has_col("APPROVED_PRICE") and has_col("SALEABLE_AREA"):
        avg_per_sft_expr = f"AVG(SAFE_DIVIDE({safe_num_expr('APPROVED_PRICE')}, NULLIF({safe_num_expr('SALEABLE_AREA')}, 0)))"
    else:
        avg_per_sft_expr = "NULL"

    # Date filter: only apply if DATE_COL exists; otherwise error clearly.
    if not has_col(DATE_COL):
        return jsonify({"error": f"Configured DATE_COL '{DATE_COL}' not found in table {BQ_TABLE}"}), 500

    sql = f"""
      SELECT
        {bookings_expr} AS bookings,
        {gross_sale_value_expr} AS gross_sale_value,
        {sale_value_expr} AS sale_value,
        {gross_amount_received_expr} AS gross_amount_received,
        {pending_demand_expr} AS pending_demand,
        {receivables_expr} AS receivables,
        {avg_per_sft_expr} AS avg_per_sft_price
      FROM {table_fqn()}
      WHERE {DATE_COL} BETWEEN @from AND @to
      {extra_where}
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp))
    row = next(iter(job.result()), None) or {}

    totals = {
        "bookings": int(row.get("bookings") or 0),
        "gross_sale_value": float(row.get("gross_sale_value") or 0),
        "sale_value": float(row.get("sale_value") or 0),
        "gross_amount_received": float(row.get("gross_amount_received") or 0),
        "pending_demand": float(row.get("pending_demand") or 0),
        "receivables": float(row.get("receivables") or 0),
        "avg_per_sft_price": (float(row.get("avg_per_sft_price")) if row.get("avg_per_sft_price") is not None else 0),
    }

    return jsonify({
        "from": frm,
        "to": to,
        "filters": filters_echo,
        "totals": totals
    })

@app.get("/soldmis/breakdown")
def soldmis_breakdown():
    frm, to, err = get_date_range()
    if err:
        return err

    group_by = request.args.get("group_by")
    if not group_by:
        return jsonify({"error": "Missing required query param: group_by"}), 400

    # Accept schema enum values
    allowed = {
        "Cluster": "CLUSTER",
        "UNIT_TYPE": "UNIT_TYPE",
        "SOURCE": "SOURCE",
        "SALE_AGREEMENT_STATUS": "SALE_AGREEMENT_STATUS",
        "LOAN_STATUS": "LOAN_STATUS",
    }
    if group_by not in allowed:
        return jsonify({"error": f"Invalid group_by. Allowed: {list(allowed.keys())}"}), 400

    col = allowed[group_by]
    if not has_col(col):
        return jsonify({"error": f"Column for group_by '{group_by}' not found in table {BQ_TABLE}"}), 500

    if not has_col(DATE_COL):
        return jsonify({"error": f"Configured DATE_COL '{DATE_COL}' not found in table {BQ_TABLE}"}), 500

    extra_where, extra_qp, _filters_echo = build_filters_where(request.args)

    sql = f"""
      SELECT
        COALESCE(NULLIF(TRIM(CAST({col} AS STRING)), ""), "UNKNOWN") AS key,
        COUNT(1) AS bookings,
        SUM({safe_num_expr('SALE_VALUE')}) AS sale_value,
        SUM({safe_num_expr('GROSS_AMOUNT_RECEIVED')}) AS gross_amount_received,
        SUM({safe_num_expr('PENDING_DEMAND')}) AS pending_demand,
        SUM({safe_num_expr('RECEIVABLES')}) AS receivables
      FROM {table_fqn()}
      WHERE {DATE_COL} BETWEEN @from AND @to
      {extra_where}
      GROUP BY key
      ORDER BY bookings DESC
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp))

    rows = []
    for r in job.result():
        rows.append({
            "key": str(r.get("key")),
            "bookings": int(r.get("bookings") or 0),
            "sale_value": float(r.get("sale_value") or 0),
            "gross_amount_received": float(r.get("gross_amount_received") or 0),
            "pending_demand": float(r.get("pending_demand") or 0),
            "receivables": float(r.get("receivables") or 0),
        })

    return jsonify({
        "from": frm,
        "to": to,
        "group_by": group_by,
        "rows": rows
    })

@app.get("/soldmis/unit")
def soldmis_unit():
    unit_no = request.args.get("unit_no")
    if not unit_no:
        return jsonify({"error": "Missing query param: unit_no"}), 400

    if not has_col("UNIT_NO"):
        return jsonify({"error": "Column UNIT_NO not found in table"}), 500

    sql = f"""
      SELECT *
      FROM {table_fqn()}
      WHERE UPPER(CAST(UNIT_NO AS STRING)) = UPPER(@unit_no)
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

    # Convert BigQuery row to dict
    record = dict(r)
    return jsonify({"unit_no": unit_no, "record": record})

@app.get("/soldmis/payments")
def soldmis_payments():
    frm, to, err = get_date_range()
    if err:
        return err

    if not has_col(DATE_COL):
        return jsonify({"error": f"Configured DATE_COL '{DATE_COL}' not found in table {BQ_TABLE}"}), 500

    # Optional: cluster filter (as in schema)
    extra_where, extra_qp, _filters_echo = build_filters_where(request.args)

    # Payment columns: PAYMENT_1..PAYMENT_20 (only include those that exist)
    payment_cols = [f"PAYMENT_{i}" for i in range(1, 21)]
    present_payment_cols = [c for c in payment_cols if has_col(c)]

    # If none exist, we still return valid schema, just zeros.
    sum_payment_total_expr = " + ".join([f"COALESCE({safe_num_expr(c)}, 0)" for c in present_payment_cols]) if present_payment_cols else "0"

    # For by_payment_index, we compute SUM per payment column
    sums_select = []
    for i, c in enumerate(payment_cols, start=1):
        if has_col(c):
            sums_select.append(f"SUM({safe_num_expr(c)}) AS p{i}")
        else:
            sums_select.append(f"0 AS p{i}")

    # units_with_payments: count distinct units where any payment > 0 (if UNIT_NO exists and any payment col exists)
    if has_col("UNIT_NO") and present_payment_cols:
        any_payment_positive = " OR ".join([f"COALESCE({safe_num_expr(c)},0) > 0" for c in present_payment_cols])
        units_with_payments_expr = f"COUNT(DISTINCT IF({any_payment_positive}, CAST(UNIT_NO AS STRING), NULL))"
    else:
        units_with_payments_expr = "0"

    sql = f"""
      SELECT
        SUM({sum_payment_total_expr}) AS payments_total,
        {units_with_payments_expr} AS units_with_payments,
        {", ".join(sums_select)}
      FROM {table_fqn()}
      WHERE {DATE_COL} BETWEEN @from AND @to
      {extra_where}
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp))
    row = next(iter(job.result()), None) or {}

    totals = {
        "payments_total": float(row.get("payments_total") or 0),
        "units_with_payments": int(row.get("units_with_payments") or 0),
    }

    by_payment_index = []
    for i in range(1, 21):
        by_payment_index.append({
            "payment_index": i,
            "total": float(row.get(f"p{i}") or 0)
        })

    return jsonify({
        "from": frm,
        "to": to,
        "totals": totals,
        "by_payment_index": by_payment_index
    })

if __name__ == "__main__":
    # Local dev only. Cloud Run uses gunicorn.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
