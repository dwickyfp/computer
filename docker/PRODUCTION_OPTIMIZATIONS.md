# Production Nginx & Container Optimizations

## Overview
This document outlines the production-ready improvements made to the nginx configuration, supervisord setup, and entrypoint script for the Rosetta web container.

## Key Improvements

### 1. Nginx Configuration (`nginx.conf`)

#### Performance Enhancements
- **Increased worker connections**: 1024 → 4096 (handles more concurrent connections)
- **Worker resource limits**: Added `worker_rlimit_nofile 65535` for high-traffic scenarios
- **Optimized keepalive**: Set to 75s with 100 requests per connection
- **Buffered logging**: 32KB buffer with 5s flush interval reduces disk I/O
- **Enhanced sendfile**: Added `sendfile_max_chunk 512k` for better large file handling
- **Disabled server tokens**: Hides nginx version for security

#### Security Headers
Added comprehensive security headers:
- `X-Frame-Options: SAMEORIGIN` - Prevents clickjacking
- `X-Content-Type-Options: nosniff` - Prevents MIME sniffing attacks
- `X-XSS-Protection` - Legacy XSS protection
- `Referrer-Policy` - Controls referrer information
- `Permissions-Policy` - Restricts browser features (geolocation, camera, etc.)
- HSTS header ready for HTTPS deployment

#### Rate Limiting
Implemented two-tier rate limiting:
- **API endpoints**: 100 req/s with 200 burst capacity
- **General traffic**: 20 req/s with 50 burst capacity
- **Connection limit**: Max 10 connections per IP
- Returns 429 status for rate limit violations

#### Caching Strategy
**Frontend Assets**:
- `index.html`: No caching (always fresh)
- JS/CSS: 1 year cache with immutable flag
- Images: 1 year cache with ETag support
- Fonts: 1 year cache with CORS headers

**API Responses**:
- No caching by default (proxy_buffering off)
- Respects upstream cache headers

#### Gzip Compression
Enhanced compression:
- Minimum length: 1000 bytes (avoids compressing tiny files)
- Added more MIME types (fonts, XML variants)
- 16 buffers of 8KB each
- Disabled for IE6

#### Proxy Improvements
- **Buffering**: Optimized with 8x32KB buffers
- **Timeouts**: 
  - Connect: 60s
  - Send: 60s
  - Read: 300s (5 min for long-running API calls)
- **Error handling**: Automatic retry on backend failures (max 2 attempts)
- **WebSocket support**: Full upgrade header handling
- **Enhanced headers**: Added X-Forwarded-Host and X-Forwarded-Port

#### Access Control
- Denies access to hidden files (`.git`, `.env`, etc.)
- Blocks sensitive files (Dockerfile, docker-compose)
- Health endpoint with no rate limit and reduced logging

#### Monitoring
Enhanced log format includes:
- Request time
- Upstream connect time
- Upstream header time
- Upstream response time

### 2. Supervisord Configuration (`supervisord.conf`)

#### Process Management
**Nginx**:
- Graceful shutdown with SIGQUIT
- 10s wait before force kill
- 3 restart attempts
- 5s stabilization time

**Uvicorn**:
- Runs as `www-data` user (not root)
- 30s graceful shutdown window
- 10s stabilization time
- Added `--timeout-keep-alive 75` to match nginx keepalive
- `PYTHONUNBUFFERED=1` for immediate log output

#### Resource Limits
- Minimum file descriptors: 1024
- Minimum processes: 200
- Proper signal propagation (killasgroup, stopasgroup)

### 3. Entrypoint Script (`entrypoint.sh`)

#### Error Handling
- Strict mode: `set -euo pipefail` (fails on undefined vars, pipe errors)
- Colored log output (INFO/WARN/ERROR)
- Validates required environment variables before starting

#### Pre-flight Checks
- Verifies frontend build exists
- Checks alembic availability before migrations
- Migration timeout (5 minutes maximum)
- Optional backend health check with retry logic

#### Visibility
- Displays system resources (CPU, memory)
- Shows uvicorn worker count
- Logs environment and mode information

## Environment Variables

### Required
- `DATABASE_URL` - Required only if `RUN_MIGRATIONS=true`

### Optional
- `MODE` - Deployment mode (default: web)
- `ENVIRONMENT` - Environment name (production/staging)
- `RUN_MIGRATIONS` - Run alembic migrations (true/false, default: false)
- `WAIT_FOR_BACKEND` - Wait for backend health check (true/false, default: false)
- `WEB_CONCURRENCY` - Number of uvicorn workers (default: 4)

## Production Checklist

### Before Deploying
- [ ] Set appropriate `WEB_CONCURRENCY` (typically 2-4 per CPU core)
- [ ] Configure proper `DATABASE_URL` if using migrations
- [ ] Review rate limits based on expected traffic
- [ ] Disable `/docs` and `/redoc` endpoints (uncomment deny blocks in nginx.conf)
- [ ] Set up SSL/TLS and uncomment HSTS header
- [ ] Configure log rotation for `/var/log/nginx/*`
- [ ] Set resource limits in Docker/Kubernetes (CPU/memory)

### HTTPS Configuration
When deploying with SSL, update nginx.conf:

```nginx
listen 443 ssl http2;
listen [::]:443 ssl http2;
ssl_certificate /path/to/cert.pem;
ssl_certificate_key /path/to/key.pem;
ssl_protocols TLSv1.2 TLSv1.3;
ssl_ciphers HIGH:!aNULL:!MD5;
ssl_prefer_server_ciphers on;

# Uncomment HSTS header in security headers section
add_header Strict-Transport-Security "max-age=31536000; includeSubDomains; preload" always;
```

### Monitoring Recommendations
- Set up log aggregation (ELK, Loki, CloudWatch)
- Monitor nginx metrics: request rate, response time, error rate
- Track uvicorn worker health and restart count
- Alert on 429 (rate limit) and 5xx errors
- Monitor memory usage and OOM conditions

## Performance Tuning

### For High Traffic (> 1000 req/s)
```nginx
worker_connections 8192;
limit_req_zone $binary_remote_addr zone=api_limit:20m rate=500r/s;
keepalive_requests 1000;
```

### For Memory-Constrained Environments
```nginx
worker_connections 2048;
proxy_buffers 4 16k;
gzip_comp_level 4;
```

### For File Upload Heavy Apps
```nginx
client_max_body_size 500M;
client_body_buffer_size 256k;
proxy_request_buffering off;
```

## Testing

### Load Testing
```bash
# Test rate limiting
ab -n 1000 -c 100 http://localhost/api/pipelines/

# Test static file serving
ab -n 10000 -c 100 http://localhost/assets/index-*.js

# Test gzip compression
curl -H "Accept-Encoding: gzip" -I http://localhost/assets/index-*.css
```

### Security Testing
```bash
# Check security headers
curl -I http://localhost/ | grep -E "X-Frame|X-Content-Type|X-XSS"

# Verify hidden file blocking
curl -I http://localhost/.env  # Should return 403

# Test rate limiting
for i in {1..200}; do curl http://localhost/api/health; done
```

## Rollback Plan
If issues occur after deployment:

1. Revert to previous nginx.conf:
   ```bash
   git checkout HEAD~1 docker/nginx.conf
   ```

2. Quick fixes:
   - Reduce rate limits if blocking legitimate traffic
   - Increase timeouts if seeing 504 errors
   - Disable rate limiting temporarily: comment out `limit_req` directives

3. Emergency disable:
   ```bash
   # Remove rate limiting entirely
   sed -i '/limit_req/d' /etc/nginx/nginx.conf
   nginx -s reload
   ```

## Resources
- [Nginx Performance Tuning](https://www.nginx.com/blog/tuning-nginx/)
- [OWASP Security Headers](https://owasp.org/www-project-secure-headers/)
- [Supervisor Documentation](http://supervisord.org/configuration.html)
