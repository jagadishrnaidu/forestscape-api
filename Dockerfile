FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
CMD ["sh", "-c", "gunicorn -b :$PORT main:app"]

@app.get("/routes")
def routes():
    return {"routes": sorted([str(r) for r in app.url_map.iter_rules()])}
