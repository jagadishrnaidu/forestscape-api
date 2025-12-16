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

# Use two tables (your dataset clearly has sales + payments)
BQ_SALES_TABLE = os.getenv("BQ_SALES_TABLE", "sales")
BQ_PAYMENTS_TABLE = os.getenv("BQ_PAYMENTS_TABLE", "payments")

# Date column used for filtering (your screenshot shows DATE exists)
DATE_COL = os.getenv("DATE_COL", "DATE")

API_KEY = os.getenv("API_KEY", "")

# ---- AUTH ----
def require_bearer_auth():
    if not API_KEY:
        return None
    auth = request.headers.get("Authorization", "")
    if auth.strip() != f"Bearer {API_KEY}":
        return jsonify({"error": "Unauthorized"}), 401
    return None

@app.before_request
def auth_guard():
    if request.path in ("/health", "/routes"):
        return None
    return require_bearer_auth()

# ---- HELPERS ----
def table_fqn(table_name: str) -> str:
    if not (BQ_PROJECT and BQ_DATASET and table_name):
        raise ValueError("Missing BQ_PROJECT / BQ_DATASET / table name env vars")
    return f"`{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"

def info_schema_fqn() -> str:
    if not (BQ_PROJECT and BQ_DATASET):
        raise ValueError("Missing BQ_PROJECT / BQ_DATASET env vars")
    return f"`{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`"

# Cache columns PER TABLE
_COLUMNS_CACHE: Dict[str, set] = {}

def get_columns(table_name: str) -> set:
    if table_name in _COLUMNS_CACHE:
        return _COLUMNS_CACHE[table_name]

    sql = f"""
      SELECT UPPER(column_name) AS column_name
      FROM {info_schema_fqn()}
      WHERE table_name = @table_name
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table_name)]
        ),
    )
    cols = {str(r["column_name"]) for r in job.result()}
    _COLUMNS_CACHE[table_name] = cols
    return cols

def has_col(table_name: str, col: str) -> bool:
    return col.upper() in get_columns(table_name)

def safe_num_expr(table_name: str, col: str) -> str:
    if not has_col(table_name, col):
        return "0"
    return f"SAFE_CAST({col} AS NUMERIC)"

def get_date_range() -> Tuple[Optional[str], Optional[str], Optional[Tuple]]:
    frm = request.args.get("from")
    to = request.args.get("to")
    if not frm or not to:
        return None, None, (jsonify({"error": "Missing required query params: from, to (YYYY-MM-DD)"}), 400)
    return frm, to, None

def build_filters_where(table_name: str, params: Dict[str, str]):
    """
    Optional filters supported by schema:
      cluster, source, unit_type, sale_agreement_status, loan_status
    Apply only if column exists in the target table.
    """
    where = []
    qp: List[bigquery.ScalarQueryParameter] = []
    filters_echo = {}

    mapping = [
        ("cluster", "Cluster"),
        ("source", "SOURCE"),
        ("unit_type", "UNIT_TYPE"),
        ("sale_agreement_status", "SALE_AGREEMENT_STATUS"),
        ("loan_status", "LOAN_STATUS"),
    ]

    for qname, col in mapping:
        val = params.get(qname)
        if val:
            filters_echo[qname] = val
            if has_col(table_name, col):
                where.append(f"UPPER(CAST({col} AS STRING)) = UPPER(@{qname})")
                qp.append(bigquery.ScalarQueryParameter(qname, "STRING", val))

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

# --- SOLDMIS SUMMARY (sales table) ---
@app.get("/soldmis/summary")
def soldmis_summary():
    frm, to, err = get_date_range()
    if err:
        return err

    table = BQ_SALES_TABLE

    if not has_col(table, DATE_COL):
        return jsonify({"error": f"Configured DATE_COL '{DATE_COL}' not found in table {table}"}), 500

    extra_where, extra_qp, filters_echo = build_filters_where(table, request.args)

    bookings_expr = "COUNT(1)"
    gross_sale_value_expr = f"SUM({safe_num_expr(table, 'GROSS_SOLD_SALE_VALUE')})"
    # Sale value might be SALE_AGREEMENT or SALE_VALUE depending on your schema. Use SALE_AGREEMENT if present.
    if has_col(table, "SALE_AGREEMENT"):
        sale_value_expr = f"SUM({safe_num_expr(table, 'SALE_AGREEMENT')})"
    else:
        sale_value_expr = f"SUM({safe_num_expr(table, 'SALE_VALUE')})"

    gross_amount_received_expr = f"SUM({safe_num_expr(table, 'GROSS_AMOUNT_RECEIVED')})"
    pending_demand_expr = f"SUM({safe_num_expr(table, 'PENDING_DEMAND')})"
    receivables_expr = f"SUM({safe_num_expr(table, 'RECEIVABLES')})"

    # avg_per_sft_price
    if has_col(table, "PER_SFT_PRICE"):
        avg_per_sft_expr = f"AVG({safe_num_expr(table, 'PER_SFT_PRICE')})"
    else:
        avg_per_sft_expr = "NULL"

    sql = f"""
      SELECT
        {bookings_expr} AS bookings,
        {gross_sale_value_expr} AS gross_sale_value,
        {sale_value_expr} AS sale_value,
        {gross_amount_received_expr} AS gross_amount_received,
        {pending_demand_expr} AS pending_demand,
        {receivables_expr} AS receivables,
        {avg_per_sft_expr} AS avg_per_sft_price
      FROM {table_fqn(table)}
      WHERE {DATE_COL} BETWEEN @from AND @to
      {extra_where}
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    row = next(iter(bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result()), None) or {}

    return jsonify({
        "from": frm,
        "to": to,
        "filters": filters_echo,
        "totals": {
            "bookings": int(row.get("bookings") or 0),
            "gross_sale_value": float(row.get("gross_sale_value") or 0),
            "sale_value": float(row.get("sale_value") or 0),
            "gross_amount_received": float(row.get("gross_amount_received") or 0),
            "pending_demand": float(row.get("pending_demand") or 0),
            "receivables": float(row.get("receivables") or 0),
            "avg_per_sft_price": float(row.get("avg_per_sft_price") or 0),
        }
    })

# --- SOLDMIS BREAKDOWN (sales table) ---
@app.get("/soldmis/breakdown")
def soldmis_breakdown():
    frm, to, err = get_date_range()
    if err:
        return err

    group_by = request.args.get("group_by")
    if not group_by:
        return jsonify({"error": "Missing required query param: group_by"}), 400

    table = BQ_SALES_TABLE

    allowed = {
        "Cluster": "Cluster",
        "UNIT_TYPE": "UNIT_TYPE",
        "SOURCE": "SOURCE",
        "SALE_AGREEMENT_STATUS": "SALE_AGREEMENT_STATUS",
        "LOAN_STATUS": "LOAN_STATUS",
    }
    if group_by not in allowed:
        return jsonify({"error": f"Invalid group_by. Allowed: {list(allowed.keys())}"}), 400

    col = allowed[group_by]

    if not has_col(table, DATE_COL):
        return jsonify({"error": f"Configured DATE_COL '{DATE_COL}' not found in table {table}"}), 500
    if not has_col(table, col):
        return jsonify({"error": f"Column for group_by '{group_by}' not found in table {table}"}), 500

    extra_where, extra_qp, _filters_echo = build_filters_where(table, request.args)

    sql = f"""
      SELECT
        COALESCE(NULLIF(TRIM(CAST({col} AS STRING)), ""), "UNKNOWN") AS key,
        COUNT(1) AS bookings,
        SUM({safe_num_expr(table, 'SALE_VALUE')}) AS sale_value,
        SUM({safe_num_expr(table, 'GROSS_AMOUNT_RECEIVED')}) AS gross_amount_received,
        SUM({safe_num_expr(table, 'PENDING_DEMAND')}) AS pending_demand,
        SUM({safe_num_expr(table, 'RECEIVABLES')}) AS receivables
      FROM {table_fqn(table)}
      WHERE {DATE_COL} BETWEEN @from AND @to
      {extra_where}
      GROUP BY key
      ORDER BY bookings DESC
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    rows = []
    for r in bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result():
        rows.append({
            "key": str(r.get("key")),
            "bookings": int(r.get("bookings") or 0),
            "sale_value": float(r.get("sale_value") or 0),
            "gross_amount_received": float(r.get("gross_amount_received") or 0),
            "pending_demand": float(r.get("pending_demand") or 0),
            "receivables": float(r.get("receivables") or 0),
        })

    return jsonify({"from": frm, "to": to, "group_by": group_by, "rows": rows})

# --- SOLDMIS UNIT (sales table) ---
@app.get("/soldmis/unit")
def soldmis_unit():
    unit_no = request.args.get("unit_no")
    if not unit_no:
        return jsonify({"error": "Missing query param: unit_no"}), 400

    table = BQ_SALES_TABLE

    if not has_col(table, "UNIT_NO"):
        return jsonify({"error": f"UNIT_NO not found in table {table}"}), 500

    sql = f"""
      SELECT *
      FROM {table_fqn(table)}
      WHERE UPPER(CAST(UNIT_NO AS STRING)) = UPPER(@unit_no)
      LIMIT 1
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("unit_no", "STRING", unit_no)]
    ))
    r = next(iter(job.result()), None)
    if not r:
        return jsonify({"error": "Unit not found"}), 404

    return jsonify({"unit_no": unit_no, "record": dict(r)})

# --- SOLDMIS PAYMENTS (payments table) ---
@app.get("/soldmis/payments")
def soldmis_payments():
    frm, to, err = get_date_range()
    if err:
        return err

    table = BQ_PAYMENTS_TABLE

    if not has_col(table, DATE_COL):
        return jsonify({"error": f"Configured DATE_COL '{DATE_COL}' not found in table {table}"}), 500

    # Schema says optional cluster filter; we’ll apply all optional filters if present & column exists
    extra_where, extra_qp, _filters_echo = build_filters_where(table, request.args)

    payment_cols = [f"PAYMENT_{i}" for i in range(1, 21)]
    present = [c for c in payment_cols if has_col(table, c)]

    sum_payment_total_expr = " + ".join([f"COALESCE({safe_num_expr(table, c)}, 0)" for c in present]) if present else "0"

    sums_select = []
    for i, c in enumerate(payment_cols, start=1):
        if has_col(table, c):
            sums_select.append(f"SUM({safe_num_expr(table, c)}) AS p{i}")
        else:
            sums_select.append(f"0 AS p{i}")

    if has_col(table, "UNIT_NO") and present:
        any_payment_positive = " OR ".join([f"COALESCE({safe_num_expr(table, c)},0) > 0" for c in present])
        units_with_payments_expr = f"COUNT(DISTINCT IF({any_payment_positive}, CAST(UNIT_NO AS STRING), NULL))"
    else:
        units_with_payments_expr = "0"

    sql = f"""
      SELECT
        SUM({sum_payment_total_expr}) AS payments_total,
        {units_with_payments_expr} AS units_with_payments,
        {", ".join(sums_select)}
      FROM {table_fqn(table)}
      WHERE {DATE_COL} BETWEEN @from AND @to
      {extra_where}
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    row = next(iter(bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result()), None) or {}

    by_payment_index = [{"payment_index": i, "total": float(row.get(f"p{i}") or 0)} for i in range(1, 21)]

    return jsonify({
        "from": frm,
        "to": to,
        "totals": {
            "payments_total": float(row.get("payments_total") or 0),
            "units_with_payments": int(row.get("units_with_payments") or 0),
        },
        "by_payment_index": by_payment_index
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
