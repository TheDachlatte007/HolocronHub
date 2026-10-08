FROM python:3.12-slim
WORKDIR /app
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install --no-cache-dir -r /app/backend/requirements.txt
COPY . /app
ARG HOLOCRON_REVISION=""
RUN python3 -m backend.build_info --write /app/build-info.json --revision "$HOLOCRON_REVISION"
EXPOSE 8787
CMD ["python3", "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8787"]
