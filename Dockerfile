FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

# Stage alpha-engine-lib from a local vendor directory.
# infrastructure/deploy.sh populates vendor/alpha-engine-lib from a
# sibling checkout (local dev) or GitHub Actions checkout (CI) before
# Docker build. Mirrors the alpha-engine-research / alpha-engine-predictor
# pattern — the private repo reaches the build context without needing a
# GitHub PAT or Docker build secret. [flow_doctor] pulls flow-doctor
# transitively for lambda/handler.py's setup_logging call. The arcticdb
# extra is intentionally NOT pulled here — only weekly_collector.py /
# builders/ use ArcticDB, and those run on EC2 (not in this Lambda image).
COPY vendor/alpha-engine-lib /tmp/alpha-engine-lib

# Install dependencies. Exclude pytest / python-dotenv / pre-installed
# Lambda runtime deps (boto3 etc.). Filter alpha-engine-lib out of
# requirements.txt and install it from the staged vendor path with the
# [flow_doctor] extra. Filtered list is written to a requirements file
# and consumed by `pip install -r` so pip's own parser handles inline
# comments — `$(grep ...)` shell substitution would word-split a
# `pkg # comment` line into separate args and pip would reject the
# bare `#` as an invalid requirement.
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir /tmp/alpha-engine-lib[flow_doctor] && \
    grep -vE "^#|^$|^pytest|^python-dotenv|^boto3|^botocore|^s3transfer|^alpha-engine-lib" requirements.txt > /tmp/req-lambda.txt && \
    pip install --no-cache-dir -r /tmp/req-lambda.txt && \
    rm -rf /root/.cache/pip /tmp/alpha-engine-lib /tmp/req-lambda.txt

# Copy application code
COPY collectors/ ${LAMBDA_TASK_ROOT}/collectors/
COPY polygon_client.py ${LAMBDA_TASK_ROOT}/
COPY weekly_collector.py ${LAMBDA_TASK_ROOT}/
COPY store/ ${LAMBDA_TASK_ROOT}/store/
COPY ssm_secrets.py ${LAMBDA_TASK_ROOT}/

# flow-doctor.yaml at LAMBDA_TASK_ROOT is loaded by setup_logging() at
# module-top of lambda/handler.py. The path resolves via:
#   os.environ.get("LAMBDA_TASK_ROOT", os.path.dirname(...)) / "flow-doctor.yaml"
COPY flow-doctor.yaml ${LAMBDA_TASK_ROOT}/

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
