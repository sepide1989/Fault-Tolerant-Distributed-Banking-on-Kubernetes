#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${S3_BUCKET:?External bucket name required}"
: "${S3_ENDPOINT:?External S3-compatible HTTPS endpoint required}"
: "${S3_ACCESS_KEY:?Required}"
: "${S3_SECRET_KEY:?Required}"
command -v helm >/dev/null || { echo 'Helm required'; exit 1; }
# Barman Cloud plugin uses cert-manager for secure operator/sidecar TLS.
helm repo add jetstack https://charts.jetstack.io --force-update
helm upgrade --install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --set crds.enabled=true --wait
helm repo add cnpg https://cloudnative-pg.github.io/charts --force-update
helm upgrade --install plugin-barman-cloud cnpg/plugin-barman-cloud --namespace cnpg-system --wait
kubectl -n banking create secret generic bank-s3-creds \
  --from-literal="ACCESS_KEY_ID=$S3_ACCESS_KEY" \
  --from-literal="ACCESS_SECRET_KEY=$S3_SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
python - <<'PY' | kubectl apply -f -
import os
from pathlib import Path
s=Path('k8s/backup/01-objectstores.template.yaml').read_text()
for key,env in [('REPLACE_BUCKET','S3_BUCKET'),('REPLACE_S3_ENDPOINT','S3_ENDPOINT')]:
    value=os.environ[env].rstrip('/')
    if env=='S3_ENDPOINT':
        from urllib.parse import urlparse
        endpoint=urlparse(value)
        if endpoint.scheme not in ('http','https') or not endpoint.netloc:
            raise SystemExit('S3_ENDPOINT must include a valid http:// or https:// scheme')
    s=s.replace(key,value)
print(s)
PY
kubectl apply -f k8s/backup/02-enable-wal-archive.yaml
kubectl apply -f k8s/backup/03-daily-backups.yaml
kubectl -n banking get objectstores,scheduledbackups,backups
cat <<'EOT'
Next: Wait for each Backup object to report a COMPLETED state.
Verify objects exist in the external bucket, including WAL archives.
Do not assume full disaster recovery until a restore-as-new-cluster test passes.
EOT
