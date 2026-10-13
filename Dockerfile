# ── Dockerfile ────────────────────────────────────────────────────────────────
# Multi-stage build, slim final image

FROM python:3.11-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim
LABEL org.opencontainers.image.description="Bitrix24 Product Catalog MCP Server"

# Non-root user
RUN adduser --disabled-password --gecos "" mcpuser
USER mcpuser

WORKDIR /app
COPY --from=builder /install /usr/local
COPY server.py .

ENV PORT=8000
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["python", "server.py"]
