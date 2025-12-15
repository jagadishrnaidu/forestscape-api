FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Cloud Run listens on 8080
ENV PORT=8080

# Start server
CMD ["gunicorn", "-b", ":8080", "main:app"]
