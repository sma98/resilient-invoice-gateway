FROM python:3.11-slim
WORKDIR /app
COPY mock_erp.py .
RUN pip install fastapi uvicorn
CMD ["uvicorn", "mock_erp:app", "--host", "0.0.0.0", "--port", "9100"]