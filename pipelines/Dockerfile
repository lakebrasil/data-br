# data-br pipelines runtime — Fargate task image.
#
# Mount: S3 raw bucket exposto via S3 Files (NFS) montado em
# `/mnt/raw` pelo Fargate via EFS volume. Pipelines abrem os arquivos
# direto desse path quando `DATA_BR_RAW_MOUNT=/mnt/raw` está set
# (definido na task definition); fallback boto3 quando rodando local
# sem mount.
#
# Build:
#   docker buildx build --platform linux/arm64 -t data-br-pipelines:dev .
# Run (local, sem mount, com perfil AWS):
#   docker run --rm \
#     -e PIPELINE=bacen -e AWS_PROFILE=<seu-profile> \
#     -e ICEBERG_WAREHOUSE=arn:aws:s3tables:us-east-1:<acct>:bucket/data-br-tables \
#     -v ~/.aws:/root/.aws:ro \
#     data-br-pipelines:dev
# Run com mount-s3 local pra parity com Fargate:
#   mount-s3 <raw-bucket-name> /tmp/data-br-mount
#   docker run --rm \
#     -e PIPELINE=bacen -e AWS_PROFILE=<seu-profile> \
#     -e ICEBERG_WAREHOUSE=arn:aws:s3tables:us-east-1:<acct>:bucket/data-br-tables \
#     -e DATA_BR_RAW_MOUNT=/mnt/raw \
#     -v ~/.aws:/root/.aws:ro \
#     -v /tmp/data-br-mount:/mnt/raw:ro \
#     data-br-pipelines:dev

FROM python:3.13-slim-bookworm

# tini = init zombie reaper (Fargate task ergonomics).
# ca-certificates pra HTTPS calls.
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Deps first pra cache layer.
COPY requirements.txt ./
RUN pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

# Código.
COPY _loaders/ ./_loaders/
COPY _pipelines/ ./_pipelines/
COPY _scripts/ ./_scripts/

# Defaults sobrescritos pela task definition Fargate.
ENV DATA_BR_RAW_BUCKET=data-br-raw \
    DATA_BR_NAMESPACE=data_br \
    AWS_REGION=us-east-1 \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["/usr/bin/tini", "--"]
# `PIPELINE` env var seleciona o módulo `_pipelines.${PIPELINE}`.
# Sources: bacen, sancoes, anp_postos, emendas, beneficios, orcamento,
# cnpj, municipios (seed), ceps (seed). Args extras viram CLI args.
CMD ["sh", "-c", "exec python -m _pipelines.${PIPELINE:-bacen} ${PIPELINE_ARGS:-}"]
