FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

# Install dependencies (exclude boto3/botocore — pre-installed in Lambda)
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir \
    $(grep -vE "^#|^$|^pytest|^python-dotenv|^boto3|^botocore|^s3transfer" requirements.txt) \
    && rm -rf /root/.cache/pip

# Copy application code
COPY collectors/ ${LAMBDA_TASK_ROOT}/collectors/
COPY polygon_client.py ${LAMBDA_TASK_ROOT}/
COPY weekly_collector.py ${LAMBDA_TASK_ROOT}/
COPY config.yaml ${LAMBDA_TASK_ROOT}/config.yaml

# Lambda handler
COPY lambda/handler.py ${LAMBDA_TASK_ROOT}/handler.py

CMD ["handler.handler"]
