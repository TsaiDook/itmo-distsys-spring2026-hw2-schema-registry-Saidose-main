FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir grpcio-tools==1.71.0

COPY proto ./proto
COPY src ./src

RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src \
    --grpc_python_out=./src \
    ./proto/schema_registry.proto

ENV PYTHONPATH=/app/src
ENV PORT=50051

EXPOSE 50051

CMD ["python", "src/server.py"]
