# Migrating from Systemd Method to Quadlet Method

## Table of Contents

1. [Overview](#overview)
2. [Why Migrate?](#why-migrate)
3. [Prerequisites](#prerequisites)
4. [Migration Strategy](#migration-strategy)
5. [Step-by-Step Migration](#step-by-step-migration)
6. [File Format Conversion](#file-format-conversion)
7. [Testing Your Migration](#testing-your-migration)
8. [Rollback Plan](#rollback-plan)
9. [Common Issues](#common-issues)
10. [Coexistence Strategy](#coexistence-strategy)

---

## Overview

This guide provides comprehensive instructions for migrating from the legacy Systemd method (using `podman generate systemd`) to the modern Quadlet method in FetchIt.

**Timeline Estimate**: 2-4 hours for small deployments, 1-2 days for large production environments

**Complexity**: Medium

**Risk Level**: Low (both methods can coexist during migration)

---

## Why Migrate?

### Quadlet Advantages

**Modern Tooling:**
- Quadlet is Podman's recommended systemd integration (4.4+)
- `podman generate systemd` is deprecated and will be removed in Podman v5+

**Simplified Management:**
- Declarative `.container` files instead of complex systemd unit files
- No need to manually generate systemd units
- Cleaner separation of concerns (container config vs systemd config)

**Better Maintainability:**
- Easier to read and understand `.container` files
- Native support for volumes and networks as first-class citizens
- Automatic systemd unit generation via Quadlet generator

**Enhanced Features:**
- Better integration with systemd dependencies
- Improved handling of multi-container applications
- Native support for pod deployments (`.pod` files)

### When NOT to Migrate

**Keep using Systemd method if:**
- Running Podman < 4.4 (Quadlet not available)
- Using highly customized systemd units with complex ExecStart/ExecStop logic
- Tight deadline and no time for thorough testing
- Team unfamiliar with Quadlet syntax

**Note**: Both methods can coexist, so migration can be gradual.

---

## Prerequisites

### System Requirements

```bash
# Verify Podman version (minimum 4.4, recommended 4.9+)
podman --version
# Expected: Podman version 4.9.0 or later

# Verify cgroup version (must be v2)
podman info --format '{{.Host.CgroupsVersion}}'
# Expected: v2

# Verify systemd version
systemctl --version
# Expected: systemd 250 or later
```

### Backup Current Configuration

```bash
# Backup current FetchIt config
cp ~/.fetchit/config.yaml ~/.fetchit/config.yaml.backup

# Backup current systemd units (root mode)
sudo tar -czf /tmp/systemd-units-backup.tar.gz /etc/systemd/system/*.service

# Backup current systemd units (user mode)
tar -czf /tmp/systemd-user-units-backup.tar.gz ~/.config/systemd/user/*.service

# Document current services
systemctl --user list-units --type=service --all > /tmp/current-services-user.txt
sudo systemctl list-units --type=service --all > /tmp/current-services-root.txt
```

### Test Environment

**Strongly recommended**: Test migration in a development/staging environment before production.

```bash
# Clone production config for testing
cp ~/.fetchit/config.yaml ~/.fetchit/config-test.yaml

# Create test git repository
mkdir -p ~/migration-test/
cd ~/migration-test/
git init
```

---

## Migration Strategy

### Approach 1: Full Migration (Recommended for Small Deployments)

**Process:**
1. Convert all `.service` files to `.container` files
2. Update FetchIt config to use Quadlet method
3. Stop services using Systemd method
4. Deploy using Quadlet method
5. Verify all services running

**Downtime**: 5-15 minutes
**Risk**: Medium
**Best for**: < 10 services, non-production, or with maintenance window

### Approach 2: Gradual Migration (Recommended for Production)

**Process:**
1. Keep existing Systemd method running
2. Add Quadlet method configuration alongside
3. Migrate services one-by-one or in small batches
4. Verify each batch before proceeding
5. Remove Systemd method configuration when complete

**Downtime**: None (rolling migration)
**Risk**: Low
**Best for**: Production environments, > 10 services, mission-critical systems

### Approach 3: Blue-Green Migration

**Process:**
1. Deploy Quadlet services on different ports/networks (blue)
2. Test thoroughly
3. Switch traffic to Quadlet services
4. Decommission Systemd services (green)

**Downtime**: None
**Risk**: Very Low
**Best for**: High-availability requirements, complex applications

---

## Step-by-Step Migration

### Phase 1: Preparation (30 minutes)

#### 1.1 Audit Current Deployment

```bash
# List all current services
systemctl --user list-units --type=service | grep -v "systemd-"

# Document service dependencies
for svc in $(systemctl --user list-units --type=service --plain --no-legend | grep -v systemd | awk '{print $1}'); do
  echo "=== $svc ==="
  systemctl --user show $svc | grep -E "(After|Before|Requires|Wants)="
done > /tmp/service-dependencies.txt
```

#### 1.2 Review Current Systemd Units

```bash
# Find all FetchIt-managed systemd units
cd ~/your-git-repo/systemd/
ls -la *.service

# Review each unit file
for unit in *.service; do
  echo "=== $unit ==="
  cat $unit
  echo ""
done
```

#### 1.3 Identify Custom Configurations

Look for:
- Custom `ExecStart` arguments
- `ExecStartPre`/`ExecStartPost` scripts
- Environment files (`EnvironmentFile=`)
- Custom dependencies (`After=`, `Requires=`)
- Resource limits (`MemoryLimit=`, `CPUQuota=`)

### Phase 2: Conversion (1-2 hours)

#### 2.1 Convert Systemd Units to Quadlet Files

**Example Conversion:**

**Before: web.service (Systemd method)**

```ini
[Unit]
Description=Web Application
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
ExecStart=/usr/bin/podman run \
  --name web \
  --detach \
  --publish 8080:80 \
  --volume web-data:/data:Z \
  --env APP_ENV=production \
  docker.io/nginx:1.25
ExecStop=/usr/bin/podman stop -t 10 web
ExecStopPost=/usr/bin/podman rm -f web
Restart=always
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
```

**After: web.container (Quadlet method)**

```ini
[Unit]
Description=Web Application
After=network-online.target
Wants=network-online.target

[Container]
Image=docker.io/nginx:1.25
ContainerName=web
PublishPort=8080:80
Volume=web-data.volume:/data:Z
Environment=APP_ENV=production

[Service]
Restart=always
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
```

**Key Changes:**

1. File extension: `.service` → `.container`
2. `[Container]` section replaces `ExecStart=podman run`
3. Remove `Type=forking`, `ExecStop`, `ExecStopPost` (handled automatically)
4. Podman arguments become `[Container]` directives
5. Keep `[Unit]`, `[Service]`, and `[Install]` sections mostly the same

#### 2.2 Convert Database Service with Volume

**Before: database.service**

```ini
[Unit]
Description=PostgreSQL Database
After=network-online.target

[Service]
Type=forking
ExecStartPre=/usr/bin/mkdir -p /var/lib/postgres-data
ExecStartPre=/usr/bin/chown -R 999:999 /var/lib/postgres-data
ExecStart=/usr/bin/podman run \
  --name postgres \
  --detach \
  --publish 5432:5432 \
  --volume postgres-data:/var/lib/postgresql/data:Z \
  --env POSTGRES_PASSWORD=secret \
  --env POSTGRES_DB=appdb \
  docker.io/postgres:15
ExecStop=/usr/bin/podman stop -t 30 postgres
ExecStopPost=/usr/bin/podman rm -f postgres
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

**After: database/postgres.container**

```ini
[Unit]
Description=PostgreSQL Database
After=network-online.target postgres-data.service

[Container]
Image=docker.io/postgres:15
ContainerName=postgres
PublishPort=5432:5432
Volume=postgres-data.volume:/var/lib/postgresql/data:Z
Environment=POSTGRES_PASSWORD=secret
Environment=POSTGRES_DB=appdb

[Service]
Restart=on-failure
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

**After: database/postgres-data.volume**

```ini
[Unit]
Description=PostgreSQL Data Volume
Before=postgres.service

[Volume]
User=999
Group=999

[Install]
WantedBy=multi-user.target
```

**Key Changes:**

1. Volume defined as separate `.volume` file
2. Ownership (`User=999`) moved to `.volume` file
3. `ExecStartPre` for directory creation removed (Podman handles this)
4. Added `After=postgres-data.service` dependency
5. `TimeoutStopSec` replaces wait time in `podman stop -t 30`

#### 2.3 Convert Multi-Container Application with Network

**Before: app-frontend.service & app-backend.service**

```ini
# app-frontend.service
[Unit]
Description=Frontend
After=app-backend.service

[Service]
Type=forking
ExecStartPre=/usr/bin/podman network exists app-network || /usr/bin/podman network create app-network
ExecStart=/usr/bin/podman run \
  --name frontend \
  --detach \
  --network app-network \
  --publish 3000:3000 \
  --env BACKEND_URL=http://backend:8000 \
  docker.io/your-org/frontend:latest
ExecStop=/usr/bin/podman stop frontend
ExecStopPost=/usr/bin/podman rm frontend
Restart=always

[Install]
WantedBy=multi-user.target
```

**After: app/frontend.container**

```ini
[Unit]
Description=Frontend
After=app.network backend.service

[Container]
Image=docker.io/your-org/frontend:latest
ContainerName=frontend
Network=app.network
PublishPort=3000:3000
Environment=BACKEND_URL=http://backend:8000

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
```

**After: app/backend.container**

```ini
[Unit]
Description=Backend API
After=app.network

[Container]
Image=docker.io/your-org/backend:latest
ContainerName=backend
Network=app.network
PublishPort=8000:8000

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
```

**After: app/app.network**

```ini
[Unit]
Description=Application Network
Before=frontend.service backend.service

[Network]
Subnet=10.88.0.0/16
Gateway=10.88.0.1

[Install]
WantedBy=multi-user.target
```

**Key Changes:**

1. Network defined as separate `.network` file
2. `ExecStartPre` for network creation removed (Quadlet handles this)
3. Network settings moved to `[Network]` section
4. Dependencies updated to reference `app.network`

### Phase 3: Update FetchIt Configuration (15 minutes)

#### 3.1 Gradual Migration Config (Coexistence)

**config.yaml with both methods:**

```yaml
targetConfigs:
- name: production
  url: https://github.com/your-org/configs
  branch: main

  # Legacy Systemd method (keep running)
  systemd:
  - name: legacy-services
    targetPath: systemd/
    glob: "*.service"
    schedule: "*/10 * * * *"
    root: true
    enable: true

  # New Quadlet method (migrate here)
  quadlet:
  - name: new-services
    targetPath: quadlet/
    glob: "*.{container,volume,network}"
    schedule: "*/10 * * * *"
    root: true
    enable: true
    restart: true
```

#### 3.2 Full Migration Config (After Complete)

**config.yaml with Quadlet only:**

```yaml
targetConfigs:
- name: production
  url: https://github.com/your-org/configs
  branch: main

  quadlet:
  - name: all-services
    targetPath: quadlet/
    glob: "*.{container,volume,network}"
    schedule: "*/10 * * * *"
    root: true
    enable: true
    restart: true
```

### Phase 4: Gradual Service Migration (2-4 hours)

#### 4.1 Migrate First Service (Test Case)

**Step 1: Create Quadlet files in git**

```bash
cd ~/your-git-repo/
mkdir -p quadlet/

# Copy converted file
cp systemd/web.service.converted quadlet/web.container

# Commit to git
git add quadlet/web.container
git commit -m "Add web.container (Quadlet migration test)"
git push
```

**Step 2: Wait for FetchIt to deploy**

```bash
# Watch FetchIt logs
podman logs -f fetchit

# Expected output:
# "Processing target: production Method: quadlet"
# "Deployed web.container to /etc/containers/systemd/"
```

**Step 3: Verify Quadlet service running**

```bash
# Check Quadlet file deployed
sudo ls -la /etc/containers/systemd/web.container

# Reload daemon (FetchIt does this automatically)
sudo systemctl daemon-reload

# Check service exists
sudo systemctl list-units --all | grep "web"

# Check service status
sudo systemctl status web.service
# Expected: Active (running)
```

**Step 4: Compare with old service**

```bash
# Check old systemd service still running
sudo systemctl status web-old.service  # If you renamed it

# Compare container behavior
podman ps | grep web
curl http://localhost:8080

# If everything works, proceed to stop old service
```

**Step 5: Stop old Systemd service**

```bash
# Stop and disable old service
sudo systemctl stop web-old.service
sudo systemctl disable web-old.service

# Remove old systemd unit file from git
cd ~/your-git-repo/
git rm systemd/web.service
git commit -m "Remove old web.service (migrated to Quadlet)"
git push
```

#### 4.2 Migrate Remaining Services

Repeat Step 4.1 for each service, in batches:

**Batch Strategy:**

1. **Low-risk services first**: Development, testing, non-critical
2. **Medium-risk services**: Staging, internal tools
3. **High-risk services last**: Production databases, critical APIs

**Between Each Batch:**

- Monitor for 24-48 hours
- Check logs for errors
- Validate service health
- Get team approval before next batch

### Phase 5: Final Cleanup (30 minutes)

#### 5.1 Remove Systemd Method from Config

```yaml
# Remove systemd section from config.yaml
targetConfigs:
- name: production
  # systemd: REMOVED
  quadlet:
  - name: all-services
    # ... (as before)
```

#### 5.2 Archive Old Systemd Units

```bash
# Move old systemd directory to archive
cd ~/your-git-repo/
git mv systemd systemd-archived
git commit -m "Archive old systemd units (migration complete)"
git push

# Or delete if you have backups
# git rm -r systemd
# git commit -m "Remove old systemd units (migration complete)"
```

#### 5.3 Verify All Services Running

```bash
# List all services
sudo systemctl list-units --type=service | grep -v "systemd-"

# Check all are from Quadlet
sudo ls -la /etc/containers/systemd/

# Verify no orphaned old services
sudo systemctl list-units --type=service --all | grep -v "systemd-" | grep -v "quadlet-"
```

---

## File Format Conversion

### Conversion Matrix

| Systemd Unit File | Quadlet File | Notes |
|-------------------|--------------|-------|
| `ExecStart=/usr/bin/podman run` | `[Container]` section | Convert arguments to directives |
| `--name foo` | `ContainerName=foo` | |
| `--publish 8080:80` | `PublishPort=8080:80` | |
| `--volume vol:/path` | `Volume=vol:/path` | Or use `.volume` file |
| `--env KEY=value` | `Environment=KEY=value` | |
| `--network net` | `Network=net` | Or use `.network` file |
| `--user 1000` | `User=1000` | |
| `--group 1000` | `Group=1000` | |
| `--memory 512m` | `Memory=512M` | |
| `--cpus 0.5` | `CPUQuota=50%` | |
| `--restart on-failure` | `[Service] Restart=on-failure` | In `[Service]` section |
| `ExecStop=/usr/bin/podman stop` | (automatic) | Handled by Quadlet |
| `ExecStopPost=/usr/bin/podman rm` | (automatic) | Handled by Quadlet |
| `Type=forking` | (automatic) | Handled by Quadlet |

### Automated Conversion Script

```bash
#!/bin/bash
# systemd-to-quadlet.sh - Convert systemd unit to Quadlet file

if [ $# -lt 1 ]; then
  echo "Usage: $0 <systemd-unit-file.service>"
  exit 1
fi

INPUT="$1"
BASENAME=$(basename "$INPUT" .service)
OUTPUT="${BASENAME}.container"

echo "Converting $INPUT to $OUTPUT..."

# Extract Description
DESCRIPTION=$(grep "^Description=" "$INPUT" | cut -d= -f2-)

# Extract After
AFTER=$(grep "^After=" "$INPUT" | cut -d= -f2-)

# Extract Wants
WANTS=$(grep "^Wants=" "$INPUT" | cut -d= -f2-)

# Extract ExecStart and parse podman arguments
EXEC_START=$(grep "^ExecStart=" "$INPUT" | sed 's/^ExecStart=//')

# Parse podman run arguments
IMAGE=$(echo "$EXEC_START" | grep -oP 'docker\\.io/[^ ]+|quay\\.io/[^ ]+' | head -1)
PORTS=$(echo "$EXEC_START" | grep -oP -- '--publish [0-9]+:[0-9]+' | sed 's/--publish /PublishPort=/')
VOLUMES=$(echo "$EXEC_START" | grep -oP -- '--volume [^ ]+' | sed 's/--volume /Volume=/')
ENVS=$(echo "$EXEC_START" | grep -oP -- '--env [^ ]+' | sed 's/--env /Environment=/')
NETWORK=$(echo "$EXEC_START" | grep -oP -- '--network [^ ]+' | sed 's/--network /Network=/')
NAME=$(echo "$EXEC_START" | grep -oP -- '--name [^ ]+' | sed 's/--name /ContainerName=/')

# Generate Quadlet file
cat > "$OUTPUT" << EOF
[Unit]
Description=$DESCRIPTION
${AFTER:+After=$AFTER}
${WANTS:+Wants=$WANTS}

[Container]
Image=$IMAGE
${NAME}
$(echo "$PORTS" | sed 's/^//')
$(echo "$VOLUMES" | sed 's/^//')
$(echo "$ENVS" | sed 's/^//')
${NETWORK}

[Service]
Restart=always
TimeoutStartSec=300

[Install]
WantedBy=multi-user.target
EOF

echo "Generated $OUTPUT"
echo "IMPORTANT: Review and test this file before deploying!"
```

**Usage:**

```bash
chmod +x systemd-to-quadlet.sh
./systemd-to-quadlet.sh web.service
# Output: web.container
```

**Note**: This script provides a starting point. Manual review and testing are required.

---

## Testing Your Migration

### Test Checklist

- [ ] Service starts successfully
- [ ] Service stops cleanly
- [ ] Service restarts properly
- [ ] Service enabled for boot
- [ ] Container visible via `podman ps`
- [ ] Port mappings work correctly
- [ ] Volume mounts are correct
- [ ] Environment variables set properly
- [ ] Network connectivity working
- [ ] Dependencies resolve correctly
- [ ] Logs accessible via `journalctl`
- [ ] Service survives host reboot (if applicable)
- [ ] Update detection works (modify .container in git)
- [ ] Restart on update works (if enabled)

### Test Commands

```bash
# Start test
sudo systemctl start web.service

# Check status
sudo systemctl status web.service

# Check container
podman ps | grep web

# Test functionality
curl http://localhost:8080

# Check logs
journalctl -u web.service -n 50

# Stop test
sudo systemctl stop web.service

# Restart test
sudo systemctl restart web.service

# Enable test
sudo systemctl enable web.service

# Reboot test (if feasible)
sudo reboot
# After reboot:
sudo systemctl status web.service
```

---

## Rollback Plan

### If Migration Fails

**Immediate Rollback:**

```bash
# 1. Stop FetchIt
podman stop fetchit

# 2. Restore backup config
cp ~/.fetchit/config.yaml.backup ~/.fetchit/config.yaml

# 3. Stop Quadlet services
sudo systemctl stop $(systemctl list-units 'quadlet-*.service' --plain --no-legend | awk '{print $1}')

# 4. Re-enable old systemd services
sudo systemctl start web-old.service
sudo systemctl enable web-old.service

# 5. Restart FetchIt
podman start fetchit
```

**If Old Services Removed:**

```bash
# 1. Restore systemd units from backup
sudo tar -xzf /tmp/systemd-units-backup.tar.gz -C /

# 2. Reload daemon
sudo systemctl daemon-reload

# 3. Re-enable services
sudo systemctl enable web.service
sudo systemctl start web.service
```

---

## Common Issues

### Issue: "No such file or directory" when deploying

**Cause**: Quadlet file path incorrect in config.yaml

**Solution**:
```yaml
# Correct path
targetPath: quadlet/  # With trailing slash

# Or
targetPath: quadlet   # Without trailing slash
```

### Issue: Service exists but doesn't start

**Cause**: Quadlet generator didn't create systemd unit

**Solution**:
```bash
# Manually trigger daemon-reload
sudo systemctl daemon-reload

# Check for Quadlet errors
journalctl -xe | grep quadlet
```

### Issue: Volume permissions denied

**Cause**: Volume ownership not set in `.volume` file

**Solution**:
```ini
# Add to .volume file
[Volume]
User=1000
Group=1000
```

### Issue: Network not found

**Cause**: Network `.network` file not deployed or service dependency missing

**Solution**:
```ini
# In .container file, add dependency
[Unit]
After=app.network

# Ensure .network file exists
# quadlet/app.network
```

---

## Coexistence Strategy

Both methods can run simultaneously during migration.

**Directory Structure:**

```
your-git-repo/
├── systemd/             # Legacy systemd units
│   ├── old-service1.service
│   └── old-service2.service
└── quadlet/             # New Quadlet files
    ├── new-service1.container
    ├── new-service2.container
    ├── data.volume
    └── app.network
```

**FetchIt config.yaml:**

```yaml
targetConfigs:
- name: production
  url: https://github.com/your-org/configs
  branch: main

  # Keep both methods active
  systemd:
  - name: legacy
    targetPath: systemd/
    glob: "*.service"
    schedule: "*/10 * * * *"
    root: true
    enable: true

  quadlet:
  - name: modern
    targetPath: quadlet/
    glob: "*.{container,volume,network}"
    schedule: "*/10 * * * *"
    root: true
    enable: true
    restart: true
```

**Migration Process:**

1. Service runs as `.service` (systemd method)
2. Create `.container` version in `quadlet/` directory
3. Test `.container` deployment
4. Rename old `.service` to `.service.old` (keeps running)
5. Validate new service working
6. Remove `.service.old` from git
7. Repeat for next service

---

## Summary

**Migration Path:**

```
Phase 1: Preparation          → 30 min
Phase 2: File Conversion      → 1-2 hours
Phase 3: Config Update        → 15 min
Phase 4: Gradual Migration    → 2-4 hours
Phase 5: Cleanup              → 30 min
-----------------------------------------
Total Time (estimate):         4-8 hours
```

**Key Takeaways:**

- Quadlet is the modern, recommended approach
- Both methods can coexist during migration
- Test thoroughly in staging before production
- Migrate in small batches for large deployments
- Keep backups and have a rollback plan
- Gradual migration minimizes risk

**Next Steps:**

1. Review this guide thoroughly
2. Test conversion with 1-2 services in staging
3. Plan migration timeline with team
4. Execute migration in phases
5. Monitor closely during and after migration
6. Document any custom configurations or issues

For questions or issues, see:
- [Quadlet Guide](./quadlet.md)
- [FetchIt Documentation](./methods.rst)
- [GitHub Issues](https://github.com/containers/fetchit/issues)

---

**Last Updated**: 2025-10-15
**FetchIt Version**: v0.0.0+ (with Quadlet support)
**Migration Status**: Production Ready
