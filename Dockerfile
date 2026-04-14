FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

# Install dependencies (exclude boto3/botocore — pre-installed in Lambda).
# alpha-engine-lib is excluded because the Phase 2 Lambda handler (lambda/handler.py)
# doesn't import from it — only weekly_collector.main() (the EC2 entry point) does.
# Including it would require wiring a GitHub PAT into the Docker build as a secret,
# which is ceremony for zero benefit here. If a future Phase 2 change imports from
# alpha-engine-lib, remove this filter and add --mount=type=secret,id=lib_token
# plus the matching deploy.yml build-secrets wiring.
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir \
    $(grep -vE "^#|^$|^pytest|^python-dotenv|^boto3|^botocore|^s3transfer|^alpha-engine-lib" requirements.txt) \
    && rm -rf /root/.cache/pip

# Copy application code
COPY collectors/ ${LAMBDA_TASK_ROOT}/collectors/
COPY polygon_client.py ${LAMBDA_TASK_ROOT}/
COPY weekly_collector.py ${LAMBDA_TASK_ROOT}/
COPY store/ ${LAMBDA_TASK_ROOT}/store/
COPY ssm_secrets.py ${LAMBDA_TASK_ROOT}/

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
