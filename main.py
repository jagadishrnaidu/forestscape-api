import os
from datetime import datetime
from flask import Flask, jsonify, request
from google.cloud import bigquery

app = Flask(__name__)
bq = bigquery.Client()

# -----------------------
# ENV
# -----------------------
BQ_PROJECT = os.getenv("BQ_PROJECT", "")
BQ_DATASET = os.getenv("BQ_DATASET", "")

BQ_SALES_TABLE = os.getenv("BQ_SALES_TABLE", "sales")
BQ_PAYMENTS_TABLE = os.getenv("BQ_PAYMENTS_TABLE", "payments")

DATE_COL_DEFAULT = os.getenv("DATE_COL", "DATE")

API_KEY = os.getenv("API_KEY", "")

# -----------------------
# AUTH
# -----------------------
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

# -----------------------
# HELPERS
# -----------------------
def table_fqn(table_name: str) -> str:
    if not (BQ_PROJECT and BQ_DATASET and table_name):
        raise ValueError("Missing BQ_PROJECT / BQ_DATASET / table name")
    return f"`{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"

_schema_cache = {}

def get_table_columns(table_name: str):
    key = (BQ_PROJECT, BQ_DATASET, table_name)
    if key in _schema_cache:
        return _schema_cache[key]

    sql = f"""
      SELECT column_name
      FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = @t
    """
    rows = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("t", "STRING", table_name)]
        ),
    ).result()

    cols = set([r["column_name"] for r in rows])
    _schema_cache[key] = cols
    return cols

def has_col(table_name: str, col: str) -> bool:
    return col in get_table_columns(table_name)

def first_existing_col(table_name: str, candidates):
    cols = get_table_columns(table_name)
    for c in candidates:
        if c in cols:
            return c
    return None

def safe_num_expr(col: str) -> str:
    return f"SAFE_CAST(NULLIF(CAST({col} AS STRING), '') AS NUMERIC)"

def get_date_range():
    frm = request.args.get("from")
    to = request.args.get("to")
    if not frm or not to:
        return None, None, (jsonify({"error": "Missing required query params: from, to (YYYY-MM-DD)"}), 400)
    return frm, to, None

def build_filters_where(table_name: str, args):
    where = []
    qp = []
    echo = {}

    mapping = [
        ("cluster", ["Cluster", "CLUSTER"], "STRING"),
        ("source", ["SOURCE"], "STRING"),
        ("unit_type", ["UNIT_TYPE"], "STRING"),
        ("sale_agreement_status", ["SALE_AGREEMENT_STATUS"], "STRING"),
        ("loan_status", ["LOAN_STATUS"], "STRING"),
        ("unit_no", ["UNIT_NO"], "STRING"),
    ]

    for param, col_candidates, bqtype in mapping:
        val = args.get(param)
        if val:
            col = first_existing_col(table_name, col_candidates)
            if col:
                where.append(f"UPPER(CAST({col} AS STRING)) = UPPER(@{param})")
                qp.append(bigquery.ScalarQueryParameter(param, bqtype, val))
                echo[param] = val

    if where:
        return " AND " + " AND ".join(where), qp, echo
    return "", qp, echo

# -----------------------
# ROUTES
# -----------------------
@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "Forestscape MIS", "ts": datetime.utcnow().isoformat()})

@app.get("/routes")
def routes():
    return {
        "routes": sorted([
            f"{r.rule} [{','.join(sorted(r.methods - {'HEAD','OPTIONS'}))}]"
            for r in app.url_map.iter_rules()
        ])
    }

# -----------------------
# SOLDMIS: SUMMARY
# -----------------------
@app.get("/soldmis/summary")
def soldmis_summary():
    frm, to, err = get_date_range()
    if err:
        return err

    sales_table = BQ_SALES_TABLE
    date_col = first_existing_col(sales_table, ["BOOKING_DATE", "DATE", DATE_COL_DEFAULT]) or DATE_COL_DEFAULT
    if not has_col(sales_table, date_col):
        return jsonify({"error": f"Configured DATE_COL '{date_col}' not found in table {sales_table}"}), 500

    extra_where, extra_qp, filters_echo = build_filters_where(sales_table, request.args)

    sale_value_col = first_existing_col(sales_table, ["SALE_AGREEMENT", "SALE_VALUE", "APPROVED_PRICE_INVENTORY_VALUE"])
    gross_sale_col = first_existing_col(sales_table, ["GROSS_SOLD_SALE_VALUE", "GROSS_SALE_VALUE_WITHOUT_GST"])
    per_sft_col = first_existing_col(sales_table, ["PER_SFT_PRICE"])

    sql = f"""
      SELECT
        COUNT(1) AS bookings,
        SUM(COALESCE({safe_num_expr(gross_sale_col)}, 0)) AS gross_sale_value,
        SUM(COALESCE({safe_num_expr(sale_value_col)}, 0)) AS sale_value,
        SUM(COALESCE({safe_num_expr("GROSS_AMOUNT_RECEIVED")}, 0)) AS gross_amount_received,
        SUM(COALESCE({safe_num_expr("PENDING_DEMAND")}, 0)) AS pending_demand,
        SUM(COALESCE({safe_num_expr("RECEIVABLES")}, 0)) AS receivables,
        AVG(COALESCE({safe_num_expr(per_sft_col)}, NULL)) AS avg_per_sft_price
      FROM {table_fqn(sales_table)}
      WHERE {date_col} BETWEEN @from AND @to
      {extra_where}
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    row = next(iter(bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result()), None)

    def f(x): return float(x or 0)

    return jsonify({
        "from": frm,
        "to": to,
        "filters": filters_echo,
        "date_col_used": date_col,
        "totals": {
            "bookings": int(row["bookings"] or 0),
            "gross_sale_value": f(row["gross_sale_value"]),
            "sale_value": f(row["sale_value"]),
            "gross_amount_received": f(row["gross_amount_received"]),
            "pending_demand": f(row["pending_demand"]),
            "receivables": f(row["receivables"]),
            "avg_per_sft_price": f(row["avg_per_sft_price"]),
        }
    })

# -----------------------
# SOLDMIS: UNIT
# -----------------------
@app.get("/soldmis/unit")
def soldmis_unit():
    unit_no = request.args.get("unit_no")
    if not unit_no:
        return jsonify({"error": "Missing query param: unit_no"}), 400

    sales_table = BQ_SALES_TABLE
    if not has_col(sales_table, "UNIT_NO"):
        return jsonify({"error": f"UNIT_NO not found in table {sales_table}"}), 500

    sql = f"""
      SELECT *
      FROM {table_fqn(sales_table)}
      WHERE UPPER(CAST(UNIT_NO AS STRING)) = UPPER(@unit_no)
      LIMIT 50
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("unit_no", "STRING", unit_no)]
        ),
    )
    rows = [dict(r) for r in job.result()]
    if not rows:
        return jsonify({"error": "Not found"}), 404
    return jsonify({"unit_no": unit_no, "record": rows[0]})

# -----------------------
# SOLDMIS: BREAKDOWN
# -----------------------
@app.get("/soldmis/breakdown")
def soldmis_breakdown():
    frm, to, err = get_date_range()
    if err:
        return err

    sales_table = BQ_SALES_TABLE
    date_col = first_existing_col(sales_table, ["BOOKING_DATE", "DATE", DATE_COL_DEFAULT]) or DATE_COL_DEFAULT
    if not has_col(sales_table, date_col):
        return jsonify({"error": f"Configured DATE_COL '{date_col}' not found in table {sales_table}"}), 500

    group_by = request.args.get("group_by")
    allowed = {
        "Cluster": ["Cluster", "CLUSTER"],
        "UNIT_TYPE": ["UNIT_TYPE"],
        "SOURCE": ["SOURCE"],
        "SALE_AGREEMENT_STATUS": ["SALE_AGREEMENT_STATUS"],
        "LOAN_STATUS": ["LOAN_STATUS"],
    }
    if group_by not in allowed:
        return jsonify({"error": f"Invalid group_by. Use one of {list(allowed.keys())}"}), 400

    group_col = first_existing_col(sales_table, allowed[group_by])
    if not group_col:
        return jsonify({"error": f"Column for group_by={group_by} not found in table {sales_table}"}), 500

    extra_where, extra_qp, filters_echo = build_filters_where(sales_table, request.args)

    sale_value_col = first_existing_col(sales_table, ["SALE_AGREEMENT", "SALE_VALUE", "APPROVED_PRICE_INVENTORY_VALUE"])

    sql = f"""
      SELECT
        COALESCE(CAST({group_col} AS STRING), "UNKNOWN") AS key,
        COUNT(1) AS bookings,
        SUM(COALESCE({safe_num_expr(sale_value_col)}, 0)) AS sale_value,
        SUM(COALESCE({safe_num_expr("GROSS_AMOUNT_RECEIVED")}, 0)) AS gross_amount_received,
        SUM(COALESCE({safe_num_expr("PENDING_DEMAND")}, 0)) AS pending_demand,
        SUM(COALESCE({safe_num_expr("RECEIVABLES")}, 0)) AS receivables
      FROM {table_fqn(sales_table)}
      WHERE {date_col} BETWEEN @from AND @to
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
            "key": r["key"],
            "bookings": int(r["bookings"] or 0),
            "sale_value": float(r["sale_value"] or 0),
            "gross_amount_received": float(r["gross_amount_received"] or 0),
            "pending_demand": float(r["pending_demand"] or 0),
            "receivables": float(r["receivables"] or 0),
        })

    return jsonify({
        "from": frm,
        "to": to,
        "group_by": group_by,
        "filters": filters_echo,
        "date_col_used": date_col,
        "rows": rows
    })

# -----------------------
# SOLDMIS: PAYMENTS
# -----------------------
@app.get("/soldmis/payments")
def soldmis_payments():
    frm, to, err = get_date_range()
    if err:
        return err

    pay_table = BQ_PAYMENTS_TABLE
    date_col = first_existing_col(pay_table, ["DATE", DATE_COL_DEFAULT]) or DATE_COL_DEFAULT
    if not has_col(pay_table, date_col):
        return jsonify({"error": f"Configured DATE_COL '{date_col}' not found in table {pay_table}"}), 500
    if not has_col(pay_table, "UNIT_NO"):
        return jsonify({"error": f"UNIT_NO not found in table {pay_table}"}), 500

    extra_where, extra_qp, filters_echo = build_filters_where(pay_table, request.args)

    payment_cols = [f"PAYMENT_{i}" for i in range(1, 21)]
    present = [c for c in payment_cols if has_col(pay_table, c)]
    if not present:
        return jsonify({"error": f"No PAYMENT_1..PAYMENT_20 columns found in table {pay_table}"}), 500

    sum_expr_total = " + ".join([f"COALESCE({safe_num_expr(c)}, 0)" for c in present])

    sql = f"""
      SELECT
        SUM({sum_expr_total}) AS payments_total,
        COUNT(DISTINCT CAST(UNIT_NO AS STRING)) AS units_with_payments
      FROM {table_fqn(pay_table)}
      WHERE {date_col} BETWEEN @from AND @to
      {extra_where}
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    row = next(iter(bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result()), None)

    by_idx = []
    for i in range(1, 21):
        c = f"PAYMENT_{i}"
        if c not in present:
            continue
        sql_i = f"""
          SELECT SUM(COALESCE({safe_num_expr(c)}, 0)) AS total
          FROM {table_fqn(pay_table)}
          WHERE {date_col} BETWEEN @from AND @to
          {extra_where}
        """
        r = next(iter(bq.query(sql_i, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result()), None)
        by_idx.append({"payment_index": i, "total": float((r and r["total"]) or 0)})

    return jsonify({
        "from": frm,
        "to": to,
        "filters": filters_echo,
        "date_col_used": date_col,
        "totals": {
            "payments_total": float(row["payments_total"] or 0),
            "units_with_payments": int(row["units_with_payments"] or 0),
        },
        "by_payment_index": by_idx
    })

# -----------------------
# ✅ SOLDMIS: RECEIVABLES (list)
# -----------------------
@app.get("/soldmis/receivables")
def soldmis_receivables():
    frm, to, err = get_date_range()
    if err:
        return err

    sales_table = BQ_SALES_TABLE
    date_col = first_existing_col(sales_table, ["BOOKING_DATE", "DATE", DATE_COL_DEFAULT]) or DATE_COL_DEFAULT
    if not has_col(sales_table, date_col):
        return jsonify({"error": f"Configured DATE_COL '{date_col}' not found in table {sales_table}"}), 500

    limit = int(request.args.get("limit", "200"))
    limit = max(1, min(limit, 1000))
    min_receivable = float(request.args.get("min_receivable", "1") or 1)

    extra_where, extra_qp, filters_echo = build_filters_where(sales_table, request.args)

    sql = f"""
      SELECT
        CAST(UNIT_NO AS STRING) AS unit_no,
        COALESCE(CAST(CUSTOMER_NAME AS STRING), "") AS customer_name,
        COALESCE(CAST(Cluster AS STRING), "") AS cluster,
        COALESCE(CAST(UNIT_TYPE AS STRING), "") AS unit_type,
        COALESCE(CAST(SOURCE AS STRING), "") AS source,
        COALESCE(CAST(SALE_AGREEMENT_STATUS AS STRING), "") AS sale_agreement_status,
        COALESCE({safe_num_expr("RECEIVABLES")}, 0) AS receivables,
        COALESCE({safe_num_expr("PENDING_DEMAND")}, 0) AS pending_demand,
        COALESCE({safe_num_expr("GROSS_AMOUNT_RECEIVED")}, 0) AS gross_amount_received
      FROM {table_fqn(sales_table)}
      WHERE {date_col} BETWEEN @from AND @to
        AND COALESCE({safe_num_expr("RECEIVABLES")}, 0) >= @min_recv
      {extra_where}
      ORDER BY receivables DESC
      LIMIT {limit}
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
        bigquery.ScalarQueryParameter("min_recv", "NUMERIC", min_receivable),
    ] + extra_qp

    rows = []
    total_in_list = 0.0
    for r in bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result():
        d = dict(r)
        d["receivables"] = float(d["receivables"] or 0)
        d["pending_demand"] = float(d["pending_demand"] or 0)
        d["gross_amount_received"] = float(d["gross_amount_received"] or 0)
        total_in_list += d["receivables"]
        rows.append(d)

    return jsonify({
        "from": frm,
        "to": to,
        "filters": filters_echo,
        "date_col_used": date_col,
        "total_receivables_in_list": total_in_list,
        "rows": rows
    })

# -----------------------
# ✅ SOLDMIS: BOOKINGS (list)
# -----------------------
@app.get("/soldmis/bookings")
def soldmis_bookings():
    frm, to, err = get_date_range()
    if err:
        return err

    sales_table = BQ_SALES_TABLE
    pay_table = BQ_PAYMENTS_TABLE

    date_col = first_existing_col(sales_table, ["BOOKING_DATE", "DATE", DATE_COL_DEFAULT]) or DATE_COL_DEFAULT
    if not has_col(sales_table, date_col):
        return jsonify({"error": f"Configured DATE_COL '{date_col}' not found in table {sales_table}"}), 500

    limit = int(request.args.get("limit", "200"))
    limit = max(1, min(limit, 1000))

    sold_only = (request.args.get("sold_only", "true").lower() in ("true", "1", "yes"))

    # auto-detect sold column (if it exists)
    sold_status_col = first_existing_col(sales_table, ["SOLD_UNSOLD_ID", "SOLD_STATUS", "STATUS"])

    extra_where, extra_qp, filters_echo = build_filters_where(sales_table, request.args)

    approved_col = first_existing_col(sales_table, ["APPROVED_PRICE_INVENTORY_VALUE", "SALE_AGREEMENT", "SALE_VALUE"])
    gross_col = first_existing_col(sales_table, ["GROSS_SOLD_SALE_VALUE", "GROSS_SALE_VALUE_WITHOUT_GST", "LIST_PRICE"])

    sold_where = ""
    if sold_only and sold_status_col:
        sold_where = f" AND UPPER(CAST({sold_status_col} AS STRING)) = 'SOLD'"

    # payments in period from payments table
    payment_cols = [f"PAYMENT_{i}" for i in range(1, 21)]
    present_pay = [c for c in payment_cols if has_col(pay_table, c)]
    can_join_payments = has_col(pay_table, "UNIT_NO") and has_col(pay_table, "DATE") and len(present_pay) > 0

    if can_join_payments:
        payments_sum_expr = " + ".join([f"COALESCE({safe_num_expr(c)}, 0)" for c in present_pay])
        payments_cte = f"""
          payments_agg AS (
            SELECT
              CAST(UNIT_NO AS STRING) AS unit_no,
              SUM({payments_sum_expr}) AS payments_received_in_period
            FROM {table_fqn(pay_table)}
            WHERE DATE BETWEEN @from AND @to
            GROUP BY unit_no
          )
        """
        join_clause = "LEFT JOIN payments_agg p ON UPPER(CAST(s.unit_no AS STRING)) = UPPER(CAST(p.unit_no AS STRING))"
        payments_select = "COALESCE(p.payments_received_in_period, 0) AS payments_received_in_period"
    else:
        payments_cte = "payments_agg AS (SELECT '' AS unit_no, 0 AS payments_received_in_period)"
        join_clause = ""
        payments_select = "0 AS payments_received_in_period"

    sql = f"""
      WITH
      sales_rows AS (
        SELECT
          COALESCE(CAST(Cluster AS STRING), '') AS cluster,
          CAST(UNIT_NO AS STRING) AS unit_no,
          COALESCE(CAST(CUSTOMER_NAME AS STRING), '') AS customer_name,
          COALESCE({safe_num_expr(approved_col)}, 0) AS approved_price,
          COALESCE({safe_num_expr(gross_col)}, 0) AS gross_price
        FROM {table_fqn(sales_table)}
        WHERE {date_col} BETWEEN @from AND @to
        {sold_where}
        {extra_where}
      ),
      {payments_cte}
      SELECT
        s.customer_name,
        s.unit_no,
        s.cluster,
        s.approved_price,
        s.gross_price,
        (s.gross_price - s.approved_price) AS discount,
        {payments_select}
      FROM sales_rows s
      {join_clause}
      ORDER BY approved_price DESC
      LIMIT {limit}
    """

    qp = [
        bigquery.ScalarQueryParameter("from", "DATE", frm),
        bigquery.ScalarQueryParameter("to", "DATE", to),
    ] + extra_qp

    rows = []
    for r in bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp)).result():
        d = dict(r)
        d["approved_price"] = float(d.get("approved_price") or 0)
        d["gross_price"] = float(d.get("gross_price") or 0)
        d["discount"] = float(d.get("discount") or 0)
        d["payments_received_in_period"] = float(d.get("payments_received_in_period") or 0)
        rows.append(d)

    return jsonify({
        "from": frm,
        "to": to,
        "filters": {**filters_echo, "sold_only": sold_only},
        "date_col_used": date_col,
        "count": len(rows),
        "rows": rows
    })

# -----------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
