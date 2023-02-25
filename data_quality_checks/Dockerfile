FROM python:3.8-slim-buster

COPY app.py requirements.txt ./


RUN python3 -m venv /app-env \
  /app-env/bin/pip install --upgrade pip  \
  && /app-env/bin/pip install --no-cache-dir -r requirements.txt

CMD ["app.lambda_handler"]
