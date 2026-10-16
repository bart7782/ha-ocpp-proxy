FROM python:3.13-alpine
RUN pip install --no-cache-dir websockets requests
COPY ocpp_proxy.py run.py /app/
CMD ["python", "-u", "/app/run.py"]
