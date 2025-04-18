#!/bin/bash

# required env vars
: "${SSH_USERNAME:?SSH_USERNAME is required but is not set or is empty}"
: "${SSH_HOST:?SSH_HOST is required but is not set or is empty}"
: "${SSH_KEY:?SSH_KEY is required but is not set or is empty}"
: "${GH_PAT:?GH_PAT is required but is not set or is empty}"
: "${DEPLOY:?DEPLOY is required but is not set or is empty}"

echo "running tests..."
runcmd('python -m pytest tests')

eccho "configuring ssh..."
pem_file="$HOME/id_rsa.pem"
echo "$SSH_KEY" > "$pem_file"
chmod 600 "$pem_file"

echo "Deploying remotely..."
ssh -o StrictHostKeyChecking=no -i "$pem_file" "$SSH_USERNAME@$SSH_HOST" bash <<EOF
set -e
cd yaat
bash deploy.sh
EOF
fi