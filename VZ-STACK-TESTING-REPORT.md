# vz-stack Docker Compose Testing Report

**Test Date:** February 22, 2026  
**Test Environment:** macOS 15.6 (Sonoma) with Apple Silicon  
**vz Version:** Development build from source  
**vz-stack:** Docker Compose-compatible multi-container orchestration

---

## Executive Summary

Testing of vz-stack against popular and common Docker Compose configurations revealed **4 bugs filed** and several working features. The system is generally functional for basic to intermediate use cases, but there are issues with secrets handling, error reporting, and health check clarity.

---

## Test Results Summary

| Category | Status | Notes |
|----------|--------|-------|
| Basic single-service | ✅ PASS | Alpine, nginx working |
| Multi-service with depends_on | ✅ PASS | Service ordering works |
| Port forwarding | ✅ PASS | Host port mapping works |
| Volume mounts | ✅ PASS | Bind mounts work |
| Custom networks | ✅ PASS | Network isolation works |
| Resource limits (deploy) | ✅ PASS | CPU/memory limits parsed |
| Environment variables | ✅ PASS | Environment injection works |
| Health checks (service_healthy) | ✅ PASS | Depends_on with condition works |
| Secrets (file-based) | ❌ FAIL | Directory sharing error |
| Health check diagnostics | ⚠️ PARTIAL | Exit code 1 but unclear why |

---

## Working Features (Detailed)

### 1. Basic Single-Service Composes ✅

**Test:** Alpine with sleep command
```yaml
services:
  api:
    image: alpine:latest
    command: ["sleep", "300"]
```

**Result:** ✅ PASS - Container started, running, exec works

### 2. Nginx with Port Forwarding ✅

```yaml
services:
  web:
    image: nginx:1.25-alpine
    ports:
      - "18080:80"
```

**Result:** ✅ PASS - Port forwarding works, nginx accessible from host at localhost:18080

### 3. Multi-Service with Dependencies ✅

```yaml
services:
  db:
    image: redis:7-alpine
  web:
    image: nginx:1.25-alpine
    depends_on:
      - db
```

**Result:** ✅ PASS - DB starts first, web starts after. DNS resolution works (web can resolve "db" hostname)

### 4. Volume Mounts ✅

```yaml
services:
  app:
    image: alpine:latest
    volumes:
      - ./data:/data
```

**Result:** ✅ PASS - Host directory mounted, files visible in container

### 5. Custom Networks ✅

```yaml
services:
  web:
    networks: [frontend]
  api:
    networks: [frontend, backend]
  db:
    networks: [backend]
```

**Result:** ✅ PASS - Network isolation works correctly:
- Web (172.20.1.3) cannot reach DB (172.20.0.3) - different subnet
- Web can reach API - same frontend network
- API can reach DB - API has interfaces on both networks

### 6. Resource Limits ✅

```yaml
services:
  app:
    image: alpine:latest
    deploy:
      resources:
        limits:
          cpus: "0.5"
          memory: 256m
```

**Result:** ✅ PASS - deploy.resources.limits is parsed correctly

### 7. Environment Variables ✅

```yaml
services:
  app:
    environment:
      - DB_HOST=db
      - API_KEY=test_key
```

**Result:** ✅ PASS - Environment variables injected into container

### 8. Health Check with service_healthy Condition ✅

```yaml
services:
  postgres:
    image: postgres:16-alpine
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      retries: 3
  app:
    depends_on:
      postgres:
        condition: service_healthy
```

**Result:** ✅ PASS - App only starts after postgres health check passes

### 9. Complex Stacks (Redis, PostgreSQL, Nginx) ✅

**Tested:**
- Redis + Alpine app connectivity
- PostgreSQL with queries via psql
- Nginx + Redis stack

**Result:** ✅ PASS - All complex stacks work correctly

---

## Bugs Found

### Bug 1: Secrets with File-Based Secrets Cause Directory Sharing Errors

**Issue ID:** vz-r48  
**Priority:** P2

**Description:** When using file-based secrets in Docker Compose files, vz stack up fails with:
```
Invalid virtual machine configuration. A directory sharing device configuration is invalid. (VZError...)
```

**Reproduction:**
```yaml
services:
  app:
    image: alpine:latest
    secrets:
      - db_password

secrets:
  db_password:
    file: ./secret-file.txt
```

**Root Cause:** Likely related to how secrets files are being passed to the VM configuration. The file path for the secret is being interpreted as requiring directory sharing, but the configuration is invalid.

---

### Bug 2: Health Check Failures Have Unclear Error Messages

**Issue ID:** vz-ud3  
**Priority:** P2

**Description:** When health checks fail, the error message only shows "exit code 1" without explaining WHY the health check failed.

**Observed behavior:**
```
{"type":"health_check_failed","stack_name":"health-test","service_name":"app","attempt":1,"error":"exit code 1"}
```

**What's needed:** Better diagnostics such as:
- "wget: not found" if command doesn't exist
- "connection refused" if service not running
- Timeout messages should specify "timed out after X seconds"

---

### Bug 3: Port Conflicts Not Clearly Reported

**Issue ID:** vz-01n  
**Priority:** P2

**Description:** When attempting to start a second stack with a port that's already in use, the error is not clearly communicated.

**Observed behavior:**
- First stack on port 18080 starts successfully
- Second stack trying to use port 18080 shows "creating" then "failed" without clear message about port conflict

**What's needed:** Explicit error message like "port 18080 is already in use by stack: stack1"

---

### Bug 4: Failed Stacks Show 'partial' Status

**Issue ID:** vz-cdy  
**Priority:** P2

**Description:** Failed stacks remain in the list with "partial" status instead of "failed".

**Observed:**
```
NAME                 STATUS           SERVICES  
bad-image-test       partial (0/1)    1         
bad-volume-test      partial (0/1)    1  
```

**What's needed:** Either a "failed" status or automatic cleanup of failed stacks after some time

---

## Network Testing Details

### Network Isolation Verification

```
Container     IP Address     Networks
-----------   ------------   --------
web           172.20.1.3     frontend
api           172.20.0.2     backend
              172.20.1.2     frontend  
db            172.20.0.3     backend
```

**Isolation Tests:**
- Web (172.20.1.3) → DB (172.20.0.3): ❌ Blocked (different subnet)
- Web (172.20.1.3) → API (172.20.1.2): ✅ Works (same subnet)
- API (172.20.0.2) → DB (172.20.0.3): ✅ Works (same subnet)

---

## Security Observations

1. **Network Isolation:** Working correctly - containers on different networks cannot communicate
2. **Container Process Separation:** Each container runs in its own VM with proper isolation
3. **Host Volume Access:** Host directory mounts work but require VirtioFS configuration

---

## Performance Notes

- Single container startup: ~4 seconds
- Two-container stack: ~10 seconds (includes dependency ordering)
- Multi-container with health checks: ~18 seconds (waits for health)

---

## Recommendations

1. **High Priority:** Fix secrets handling - blocking production use cases
2. **High Priority:** Improve health check error messages
3. **Medium Priority:** Better port conflict detection
4. **Low Priority:** Improve stack status display for failed stacks

---

## Test Files Created

Test configurations are located at `/tmp/vz-tests/` (cleaned up after testing).

---

## Conclusion

vz-stack is functional for most Docker Compose use cases on macOS with VZ Linux VMs. The core functionality works - services start, networks communicate, ports forward, and volumes mount. However, secrets handling is a blocking issue that needs to be fixed before production use, and error message improvements would significantly improve developer experience.