FROM python:3.13.7-bookworm

WORKDIR /app
COPY ./requirements.txt /app/requirements.txt
COPY ./main.py /app/main.py
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
COPY ./chartop_server /app/chartop_server

WORKDIR /packages
COPY ./packages/pva_tsdb_connector-1.0.0-py3-none-any.whl /packages
RUN pip install --no-cache-dir /packages/pva_tsdb_connector-1.0.0-py3-none-any.whl

WORKDIR /app
EXPOSE 8443/tcp
CMD ["uvicorn", "main:app", "--host", "0.0.0.0",  "--port", "8443", "--ssl-keyfile", "./chartop_server/.ssl/chartop.app.key", "--ssl-certfile", "./chartop_server/.ssl/chartop.app.pem"]