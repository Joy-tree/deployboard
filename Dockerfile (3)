# ── DeployBoard Orchestrator ─────────────────────────────────────────
# Each user app runs in its OWN Docker container (sibling containers).
# This container only orchestrates — it never runs user code directly.
# Docker socket is mounted so we can spawn/manage sibling containers.

FROM node:20-alpine

# git for cloning repos, docker CLI for spawning app containers
RUN apk add --no-cache git docker-cli curl

WORKDIR /app

COPY package*.json ./
RUN npm install --only=production && npm cache clean --force

COPY . .

# Directories for sites data and temp builds
RUN mkdir -p /var/www/user-sites /tmp/deployboard-builds

EXPOSE 3001

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:3001/api/health || exit 1

CMD ["node", "server.js"]
