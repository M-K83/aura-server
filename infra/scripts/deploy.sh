#!/usr/bin/env bash
set -e

echo "🚀 Deploying Aura..."

git pull --rebase origin main

docker compose --env-file .env.runtime -f infra/docker/compose.yml build
docker compose --env-file .env.runtime -f infra/docker/compose.yml up -d


echo "✅ Aura deployed successfully"
