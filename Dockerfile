# ==========================================
# GLOBAL ARGS (Change versions here)
# ==========================================
ARG NODE_VERSION=24.12.0
ARG BUN_VERSION=1.3.2
ARG CADDY_VERSION=latest

# ==========================================
# Stage 1: Get Bun Binary
# ==========================================
FROM oven/bun:${BUN_VERSION}-slim AS bun-source

# ==========================================
# Stage 2: Build Caddy with DuckDNS
# ==========================================
FROM caddy:builder AS caddy-builder
RUN xcaddy build \
    --with github.com/caddy-dns/duckdns

# ==========================================
# Stage 3: Build Next.js App
# ==========================================
FROM node:${NODE_VERSION}-slim AS next-builder

# Install Python and Build Tools for C++ modules (like utp-native)
RUN apt-get update && \
    apt-get install -y python3 make g++ build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install Bun by copying it from Stage 1
COPY --from=bun-source /usr/local/bin/bun /usr/local/bin/bun

WORKDIR /app
COPY package.json bun.lock ./

# Install dependencies using Bun
RUN bun install --frozen-lockfile

COPY . .

# Build the app (You can use 'bun run build' or 'npm run build')
RUN bun run build

# ==========================================
# Stage 4: Final Production Image
# ==========================================
FROM node:${NODE_VERSION}-slim AS runner

# Install Supervisor
RUN apt-get update && apt-get install -y supervisor && rm -rf /var/lib/apt/lists/*
COPY supervisord.conf /etc/supervisord.conf

# Copy Caddy Binary
COPY --from=caddy-builder /usr/bin/caddy /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile
ENV XDG_DATA_HOME=/caddy/data
ENV XDG_CONFIG_HOME=/caddy/config

WORKDIR /app

# Copy Next.js Artifacts
COPY --from=next-builder /app/package.json ./package.json
COPY --from=next-builder /app/node_modules ./node_modules
COPY --from=next-builder /app/.next ./.next
COPY --from=next-builder /app/dist ./dist

# Expose and Run
EXPOSE 3000 443
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]