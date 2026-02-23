# vz-stack UI/UX Wireframes

## Issue 1: Stack List Status is Confusing

### Current Output
```
NAME                 STATUS           SERVICES  
----------------------------------------------
my-stack            partial (0/1)    1         
another-stack       running          2         
failed-stack        partial (0/1)    1  
```

### Problems
- "partial (0/1)" is unclear - what does it mean?
- No indication of why a stack failed
- Can't distinguish between "starting" and "failed"

---

### Proposed Wireframe: `vz stack ls`

```
vz stack ls

STACK NAME          STATUS        READY/TOTAL    PORTS               AGE
──────────────────  ────────────  ─────────────  ──────────────────  ───
my-app              ● running     2/2            8080->80, 3000      2m
web-api             ◐ starting    1/2            8081->80            10s
payment-service     ○ failed      0/1            -                   5m
old-stack           ◌ stopped     0/1            -                   1h

Legend: ● running  ◐ starting  ◌ stopped  ○ failed  ⊗ partial

Showing 4 stacks (1 failed, use 'vz stack logs <name>' for details)
```

**Improvements:**
- Clear status icons with color
- Shows ready/total count
- Shows exposed ports
- Shows age of stack
- Legend explaining symbols
- Helpful hint about failed stacks

---

## Issue 2: Service Status (vz stack ps) Lacks Information

### Current Output
```
SERVICE              STATUS         CONTAINER ID                            
--------------------------------------------------------------------------
api                  running        api                                     
web                  running        web                                     
db                   running (ready) db                                      
```

### Problems
- No port information
- No health status unless explicitly "(ready)"
- No resource usage
- Container ID is just the service name

---

### Proposed Wireframe: `vz stack ps <stack>`

```
vz stack ps my-app

SERVICE   STATUS      HEALTH       PORTS                 CONTAINER    CPU/MEM
────────  ──────────  ──────────   ────────────────────  ──────────  ────────
api       ● running   ✓ healthy    3000:3000             api          2% / 64MB
web       ● running   -            8080:80 → 80          web          1% / 32MB
postgres  ● running   ✓ healthy    5432:5432 → 5432     postgres     3% / 128MB
redis     ◌ stopped   -            -                     -            -

Use 'vz stack logs my-app' for full logs
Use 'vz stack exec my-app <service>' to run commands
```

**Improvements:**
- Clear status icon
- Health status with checkmark or X
- Full port mapping visible (host->container)
- Resource usage (CPU/MEM)
- Better visual hierarchy

---

## Issue 3: Health Check Errors Are Unhelpful

### Current Output (from events)
```json
{"type":"health_check_failed","stack_name":"health-test","service_name":"app","attempt":1,"error":"exit code 1"}
```

### Problems
- Only shows "exit code 1" - no context
- Can't see what command was run
- Can't see stdout/stderr
- No suggestion for how to fix

---

### Proposed Wireframe: Health Check Failure Display

```
vz stack ps health-app

SERVICE   STATUS      HEALTH       LAST CHECK        ERROR
────────  ──────────  ──────────   ────────────────  ────────────────────────────────
app       ● running   ✗ unhealthy  5s ago            wget: not found (exit code 127)

⚠ Health check failed 3 consecutive times

Last 3 attempts:
  1. [5s ago] wget -q --spider http://localhost/ → exit code 1
  2. [10s ago] wget -q --spider http://localhost/ → exit code 1
  3. [15s ago] wget -q --spider http://localhost/ → exit code 1

Suggestions:
  • Ensure 'wget' is installed in the container
  • Check that the service is listening on the correct port
  • Use a different test: ["CMD-SHELL", "curl -f http://localhost/"]

Full logs: vz stack logs health-app --health-checks
```

**Improvements:**
- Shows actual command that was run
- Shows error message from command
- Shows last N attempts with timestamps
- Provides actionable suggestions
- Color-coded for quick scanning

---

## Issue 4: Port Conflict Errors

### Current Output
```
web: Creating...
web: Failed: 
[✘] Stack failed — 0 ready, 1 failed (3.9s)
```

### Problems
- No indication the port is the issue
- Empty "Failed:" message
- User has to manually check what's wrong

---

### Proposed Wireframe: Port Conflict Error

```
vz stack up -f web.yaml

web: Creating...
web: Failed: port 8080 already in use
[✘] Stack failed — 0 ready, 1 failed (3.9s)

Error Details:
  ─────────────────────────────────────────────────────────────
  Service: web
  Port:    8080:80
  Error:   Address already in use
  
  Currently using port 8080:
    • Stack: 'my-website' (service: web)
    • Process: nginx (PID: 12345)
  
  Solutions:
    1. Stop the conflicting stack: vz stack down my-website
    2. Use a different host port: ports: "8081:80"
    3. Release port 8080: lsof -ti:8080 | xargs kill
  ─────────────────────────────────────────────────────────────
```

**Improvements:**
- Clear "port already in use" message
- Shows what's using the port
- Provides actionable solutions

---

## Issue 5: Failed Stack Display

### Current Output
```
NAME              STATUS         SERVICES  
────────────────────────────────────────────
failed-stack     partial (0/1)  1  
```

### Problems
- "partial" is technical jargon
- No indication of WHY it failed
- User must dig through logs

---

### Proposed Wireframe: `vz stack ls` with Failed Stacks

```
vz stack ls

STACK NAME       STATUS    READY/TOTAL   ERROR SUMMARY
──────────────── ────────  ────────────  ───────────────────────────────────
my-app           ● running  2/2          -
web-api          ● running  1/1          -
problem-stack    ○ failed   0/1          Health check failed after 3 attempts
partial-stack    ⊗ partial  1/2          1 service failed to start

Showing 4 stacks (1 failed, 1 partial)

To see details:
  vz stack ps problem-stack    # Show service status
  vz stack logs problem-stack  # Show error logs
  vz stack events problem-stack # Show lifecycle events
```

**Improvements:**
- Clear "failed" status with error summary
- Shows partial stacks with explanation
- Actionable next steps

---

## Issue 6: Better Progress During Stack Up

### Current Output
```
api: Creating...
api: Started (0.0s)
web: Creating...
web: Started (0.0s)
[✔] Stack ready — 2 services (4.4s)
```

### Problems
- No indication of what's happening during "Creating..."
- No progress for image pulls
- All happens very fast but confusing when slow

---

### Proposed Wireframe: `vz stack up` with Progress

```
vz stack up -f compose.yaml

⬡ my-app stack starting...

Services:
  ⏳ postgres   Pulling image postgres:16-alpine... (2/3)
  ⏳ api        Waiting for postgres (dependency)
  ⏳ web        Waiting for api (dependency)

  → Pulling postgres:16-alpine (45.2 MB)
    └─ Downloading: ████████████░░░░░░░░░░░░ 67% (30.2 MB/s)

⬡ postgres ready → starting api...
  ⏳ api   Creating container...
  ⏳ web   Waiting for api

⬡ api ready → starting web...

[✔] Stack ready — 3 services (18.4s)

Service    URL                      Status
─────────  ────────────────────────  ────────────
postgres   -                        ● healthy
api        http://localhost:3000    ● running
web        http://localhost:8080    ● running

Run 'vz stack logs my-app' for logs, 'vz dashboard' for live view
```

---

## Issue 7: Dashboard Improvements

Let me check what the current dashboard looks like:
<minimax:tool_call>
<invoke name="grep">