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
COPY store/ ${LAMBDA_TASK_ROOT}/store/

# NOTE: config.yaml is intentionally NOT copied here. It is gitignored
# (contains bucket names + prefixes that we keep out of the public repo)
# so there's nothing to copy at build time, and lambda/handler.py already
# falls back to a hardcoded default ({"bucket": "alpha-engine-research",
# "market_data": {"s3_prefix": "market_data/"}}) when config.yaml is
# absent. Including it here was a dead COPY that broke `docker build`
# on every fresh checkout and blocked the auto-deploy workflow.

# Lambda handler
COPY lambda/handler.py ${LAMBDA_TASK_ROOT}/handler.py

CMD ["handler.handler"]
