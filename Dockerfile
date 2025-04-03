FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY mom_server /app/mom_server
CMD ["uvicorn", "mom_server.main:app", "--host", "0.0.0.0", "--port", "8000"]
