FROM python:3.11-slim

WORKDIR /app

# Install the LEDSAS SDK (from PyPI) plus any service deps.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Run as a non-root user.
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python", "main.py"]
