# FetchIt Quadlet Method - Comprehensive Guide

## Table of Contents

1. [Introduction](#introduction)
2. [What is Quadlet?](#what-is-quadlet)
3. [Requirements](#requirements)
4. [Quick Start](#quick-start)
5. [Configuration Reference](#configuration-reference)
6. [Quadlet File Syntax](#quadlet-file-syntax)
7. [Deployment Modes](#deployment-modes)
8. [Change Detection and Updates](#change-detection-and-updates)
9. [Service Lifecycle Management](#service-lifecycle-management)
10. [Examples](#examples)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)
13. [Migration Guide](#migration-guide)

---

## Introduction

The Quadlet method is the modern, recommended approach for managing Podman containers with systemd integration in FetchIt. Introduced in Podman 4.4+, Quadlet provides declarative container management through systemd, replacing the deprecated `podman generate systemd` command.

**Key Benefits:**

- **Declarative Configuration**: Define containers using `.container`, `.volume`, and `.network` files
- **Native systemd Integration**: Automatic service generation and lifecycle management
- **GitOps Friendly**: Track container configurations in git repositories
- **Rootless Support**: Run containers as regular users without root privileges
- **Automatic Updates**: FetchIt synchronizes git changes to deployed services

---

## What is Quadlet?

Quadlet is Podman's native systemd integration feature that converts Quadlet unit files into systemd service units automatically. When you place Quadlet files in specific directories and run `systemctl daemon-reload`, the Quadlet generator creates corresponding systemd units.

**Quadlet File Types:**

- **`.container`**: Defines a container (similar to `podman run`)
- **`.volume`**: Defines a persistent volume
- **`.network`**: Defines a custom network
- **`.kube`**: Defines a Kubernetes YAML deployment (not covered in this guide)
- **`.pod`**: Defines a Podman pod (not covered in this guide)

**How FetchIt Uses Quadlet:**

1. FetchIt monitors git repositories for Quadlet files
2. On changes, FetchIt copies files to systemd directories
3. FetchIt runs `systemctl daemon-reload` to trigger Quadlet generator
4. Quadlet generator creates systemd units from `.container`/`.volume`/`.network` files
5. FetchIt enables/starts/restarts services based on configuration

---

## Requirements

### System Requirements

**Operating System** (one of):
- RHEL 9+
- Fedora 39+
- Ubuntu 22.04+
- Debian 12+
- CentOS Stream 9+

**Software Requirements:**

```bash
# Podman (minimum 4.4, recommended 4.9+)
podman --version
# Output: Podman version 4.9.0 or later

# systemd (minimum 250+)
systemctl --version
# Output: systemd 250 or later

# cgroup v2 (required)
podman info --format '{{.Host.CgroupsVersion}}'
# Output: v2

# For building FetchIt
go version
# Output: go version go1.22.0 or later
```

### Installation

**Install Podman 4.9+ (if not already installed):**

```bash
# RHEL/CentOS/Fedora
sudo dnf install -y podman

# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y podman

# Enable Podman socket
sudo systemctl enable --now podman.socket
```

**Check cgroup version:**

```bash
podman info --format '{{.Host.CgroupsVersion}}'
```

**If cgroup v1 (requires migration to v2):**

```bash
# Add kernel parameter
sudo grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=1"

# Reboot system
sudo reboot
```

---

## Quick Start

### 1. Create a Test Git Repository

```bash
mkdir -p ~/container-configs/quadlet
cd ~/container-configs

# Create a simple nginx.container file
cat > quadlet/nginx.container << 'EOF'
[Unit]
Description=Nginx Web Server
After=network-online.target

[Container]
Image=docker.io/nginx:latest
PublishPort=8080:80

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
EOF

# Initialize git repository
git init
git add .
git commit -m "Add nginx Quadlet file"
```

### 2. Create FetchIt Configuration

```bash
mkdir -p ~/.fetchit

# Create config.yaml
cat > ~/.fetchit/config.yaml << 'EOF'
targetConfigs:
- name: my-containers
  url: file:///home/YOUR_USERNAME/container-configs
  branch: main

  quadlet:
  - name: web-services
    targetPath: quadlet/
    glob: "*.container"
    schedule: "*/5 * * * *"  # Check every 5 minutes
    root: false              # User mode (no root required)
    enable: true             # Enable services on boot
    restart: true            # Restart on updates
EOF
```

### 3. Run FetchIt

**User Mode (Rootless):**

```bash
# Enable lingering (so services persist after logout)
loginctl enable-linger $USER

# Create required directories
mkdir -p ~/.config/containers/systemd

# Run FetchIt
podman run -d \
  --name fetchit \
  -v ~/.fetchit/config.yaml:/opt/mount/config.yaml:Z \
  -v ~/.config/containers/systemd:/home/fetchit/.config/containers/systemd:Z \
  -v $XDG_RUNTIME_DIR/podman/podman.sock:/run/podman/podman.sock:Z \
  -v $XDG_RUNTIME_DIR/systemd:/run/user/$(id -u)/systemd:Z \
  -e HOME=/home/fetchit \
  -e XDG_RUNTIME_DIR=/run/user/$(id -u) \
  --privileged \
  quay.io/fetchit/fetchit:latest

# Watch logs
podman logs -f fetchit
```

### 4. Verify Deployment

```bash
# Check deployed files
ls ~/.config/containers/systemd/
# Output: nginx.container

# Check service status
systemctl --user status nginx.service
# Output: Active (running)

# Test nginx
curl http://localhost:8080
# Output: Nginx welcome page
```

---

## Configuration Reference

### FetchIt config.yaml Structure

```yaml
targetConfigs:
- name: string                    # Unique target identifier
  url: string                     # Git repository URL
  branch: string                  # Git branch to track

  quadlet:
  - name: string                  # Unique method name within target
    targetPath: string            # Path in git repo (e.g., "quadlet/")
    glob: string                  # File pattern (e.g., "*.container")
    schedule: string              # Cron expression (e.g., "*/10 * * * *")
    skew: integer                 # Random delay in ms (optional)
    root: boolean                 # Root vs user mode
    enable: boolean               # Enable services on boot
    restart: boolean              # Restart on updates (implies enable=true)
```

### Field Descriptions

#### name (required)
Unique identifier for this Quadlet method instance within the target.

**Valid values**: Alphanumeric, hyphens, underscores (1-64 chars)
**Example**: `web-services`, `database-prod`, `monitoring`

#### targetPath (optional)
Relative path within git repository containing Quadlet files.

**Default**: `""` (repository root)
**Example**: `quadlet/`, `production/containers/`, `services/web/`

#### glob (optional)
Glob pattern to filter files in targetPath.

**Default**: `*` (all files)
**Examples**:
- `*.container` - Only .container files
- `web-*.container` - Only web-prefixed .container files
- `*.{container,volume,network}` - All Quadlet file types

#### schedule (required)
Cron expression for periodic git synchronization.

**Format**: 5 or 6 fields (minute hour day month weekday [second])
**Examples**:
- `*/5 * * * *` - Every 5 minutes
- `0 2 * * *` - Daily at 2 AM
- `0 */4 * * *` - Every 4 hours
- `@hourly` - Shorthand for hourly

#### root (required)
Determines deployment location and systemd context.

**If true**: Deploy to `/etc/containers/systemd/` (system-wide, requires root)
**If false**: Deploy to `~/.config/containers/systemd/` (user mode, rootless)

#### enable (optional)
Controls whether services are enabled for automatic start on boot.

**Default**: `false`
**If true**: Runs `systemctl enable <service>` after deployment
**If false**: Only deploys files, does not enable services

#### restart (optional)
Controls whether services are restarted when Quadlet files are updated.

**Default**: `false`
**If true**: Runs `systemctl restart <service>` on updates (implies `enable=true`)
**If false**: Updates files but does not restart running services

---

## Quadlet File Syntax

### .container File Structure

A `.container` file defines a container deployment.

**Example: nginx.container**

```ini
[Unit]
Description=Nginx Web Server
After=network-online.target
Wants=network-online.target

[Container]
Image=docker.io/nginx:latest
PublishPort=8080:80
PublishPort=8443:443
Volume=nginx-data.volume:/usr/share/nginx/html:Z
Network=webapp.network
Environment=NGINX_HOST=localhost
Environment=NGINX_PORT=80
User=1000
Group=1000

[Service]
Restart=always
TimeoutStartSec=300
ExecStartPre=/usr/bin/echo "Starting Nginx"

[Install]
WantedBy=multi-user.target default.target
```

**[Unit] Section:**
- `Description`: Human-readable service description
- `After`: Start after these systemd units
- `Wants`: Soft dependency (doesn't fail if unavailable)
- `Requires`: Hard dependency (fails if unavailable)

**[Container] Section:**
- `Image`: Container image to use (required)
- `PublishPort`: Port mappings (host_port:container_port)
- `Volume`: Volume mounts (volume_name:mount_path[:options])
- `Network`: Network to connect to
- `Environment`: Environment variables
- `User`: Run as specific UID
- `Group`: Run as specific GID
- `PodmanArgs`: Additional podman arguments

**[Service] Section:**
- `Restart`: Restart policy (always, on-failure, unless-stopped)
- `TimeoutStartSec`: Maximum time to start (default 90s)
- `ExecStartPre`: Run before starting container
- `ExecStartPost`: Run after starting container

**[Install] Section:**
- `WantedBy`: Enable service for these targets

### .volume File Structure

A `.volume` file defines a persistent volume.

**Example: webapp-data.volume**

```ini
[Unit]
Description=Web Application Data Volume
Before=webapp.service

[Volume]
User=1000
Group=1000
Label=app=webapp
Label=environment=production

[Install]
WantedBy=multi-user.target
```

**[Volume] Section:**
- `User`: Volume owner UID
- `Group`: Volume owner GID
- `Label`: Key=value labels for the volume

### .network File Structure

A `.network` file defines a custom network.

**Example: webapp.network**

```ini
[Unit]
Description=Web Application Network
Before=webapp.service

[Network]
Subnet=10.88.0.0/16
Gateway=10.88.0.1
IPv6=false
Driver=bridge
Label=app=webapp
Label=tier=frontend

[Install]
WantedBy=multi-user.target
```

**[Network] Section:**
- `Subnet`: Network subnet (CIDR notation)
- `Gateway`: Gateway IP address
- `IPv6`: Enable IPv6 (true/false)
- `Driver`: Network driver (bridge, macvlan, etc.)
- `Label`: Key=value labels for the network

---

## Deployment Modes

### User Mode (Rootless)

**Use Case**: Regular users deploying their own containers without root privileges.

**Configuration:**

```yaml
quadlet:
- name: user-services
  root: false  # User mode
  enable: true
  restart: true
```

**Deployment Location**: `~/.config/containers/systemd/`

**systemd Context**: User systemd (`systemctl --user`)

**Requirements:**
- `loginctl enable-linger $USER` (services persist after logout)
- `$HOME` environment variable set
- `$XDG_RUNTIME_DIR` set (usually `/run/user/$(id -u)`)
- User systemd running

**Service Management:**

```bash
# Check status
systemctl --user status nginx.service

# Start/stop/restart
systemctl --user start nginx.service
systemctl --user stop nginx.service
systemctl --user restart nginx.service

# Enable/disable
systemctl --user enable nginx.service
systemctl --user disable nginx.service

# View logs
journalctl --user -u nginx.service -f
```

**Container Management:**

```bash
# List containers
podman ps

# View logs
podman logs nginx

# Inspect container
podman inspect nginx
```

### Root Mode (System-wide)

**Use Case**: System administrators deploying containers for all users.

**Configuration:**

```yaml
quadlet:
- name: system-services
  root: true  # Root mode
  enable: true
  restart: true
```

**Deployment Location**: `/etc/containers/systemd/`

**systemd Context**: System systemd (`systemctl`)

**Requirements:**
- FetchIt running as root
- Write access to `/etc/containers/systemd/`
- System systemd running

**Service Management:**

```bash
# Check status
sudo systemctl status nginx.service

# Start/stop/restart
sudo systemctl start nginx.service
sudo systemctl stop nginx.service
sudo systemctl restart nginx.service

# Enable/disable
sudo systemctl enable nginx.service
sudo systemctl disable nginx.service

# View logs
sudo journalctl -u nginx.service -f
```

**Container Management:**

```bash
# List containers (root)
sudo podman ps

# View logs
sudo podman logs nginx

# Inspect container
sudo podman inspect nginx
```

---

## Change Detection and Updates

FetchIt automatically detects changes in your git repository and applies them to deployed Quadlet files.

### Change Types

**1. Create (New File)**

When a new `.container`/`.volume`/`.network` file is added to git:

- FetchIt copies the file to the systemd directory
- Runs `systemctl daemon-reload`
- If `enable=true`: Runs `systemctl enable <service>`
- If `enable=true`: Runs `systemctl start <service>`

**2. Update (File Modified)**

When an existing Quadlet file is modified in git:

- FetchIt copies the updated file to the systemd directory
- Runs `systemctl daemon-reload`
- If `restart=true`: Runs `systemctl restart <service>`
- If `restart=false`: Service continues with old configuration (restart manually)

**3. Rename (File Renamed)**

When a Quadlet file is renamed in git:

- FetchIt stops the old service
- Copies the new file with the new name
- Runs `systemctl daemon-reload`
- If `enable=true`: Enables and starts the new service

**4. Delete (File Removed)**

When a Quadlet file is removed from git:

- FetchIt stops the service
- Removes the file from the systemd directory
- Runs `systemctl daemon-reload`
- Note: Associated volumes and networks are NOT automatically deleted

### Update Flow Example

```bash
# Initial state: nginx running on port 8080
systemctl --user status nginx.service
# Status: Active (running)

# Update nginx.container in git (change port to 9090)
cd ~/container-configs
sed -i 's/8080:80/9090:80/' quadlet/nginx.container
git commit -am "Change nginx port to 9090"

# Wait for FetchIt schedule (e.g., */5 * * * *)
# FetchIt detects change, updates file, restarts service

# Verify update applied
curl http://localhost:9090
# Output: Nginx welcome page

curl http://localhost:8080
# Output: Connection refused (old port no longer mapped)
```

---

## Service Lifecycle Management

### Service States

**Active (running)**: Service is currently running
**Inactive (dead)**: Service is stopped
**Failed**: Service failed to start or crashed
**Activating**: Service is starting
**Deactivating**: Service is stopping

### Manual Service Management

```bash
# User mode
systemctl --user status <service>
systemctl --user start <service>
systemctl --user stop <service>
systemctl --user restart <service>
systemctl --user enable <service>
systemctl --user disable <service>

# Root mode
sudo systemctl status <service>
sudo systemctl start <service>
sudo systemctl stop <service>
sudo systemctl restart <service>
sudo systemctl enable <service>
sudo systemctl disable <service>
```

### Viewing Logs

```bash
# Follow logs in real-time
journalctl --user -u nginx.service -f

# View last 50 lines
journalctl --user -u nginx.service -n 50

# View logs since last boot
journalctl --user -u nginx.service -b

# View logs with specific priority
journalctl --user -u nginx.service -p err
```

### Debugging Service Failures

```bash
# Check service status with details
systemctl --user status nginx.service -l

# View recent logs
journalctl --user -u nginx.service -n 100

# Check container logs
podman logs <container-name-or-id>

# Inspect container
podman inspect <container-name-or-id>

# Check Quadlet file syntax
cat ~/.config/containers/systemd/nginx.container

# Manually reload daemon
systemctl --user daemon-reload
```

---

## Examples

### Example 1: Simple Web Server

**File: web/nginx.container**

```ini
[Unit]
Description=Nginx Web Server
After=network-online.target

[Container]
Image=docker.io/nginx:latest
PublishPort=8080:80

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
```

**FetchIt config.yaml:**

```yaml
targetConfigs:
- name: web-services
  url: https://github.com/your-org/containers
  branch: main

  quadlet:
  - name: nginx
    targetPath: web/
    glob: "*.container"
    schedule: "*/10 * * * *"
    root: false
    enable: true
    restart: true
```

### Example 2: Database with Volume

**File: database/postgres.container**

```ini
[Unit]
Description=PostgreSQL Database
After=network-online.target postgres-data.service

[Container]
Image=docker.io/postgres:15
PublishPort=5432:5432
Volume=postgres-data.volume:/var/lib/postgresql/data:Z
Environment=POSTGRES_PASSWORD=mysecretpassword
Environment=POSTGRES_USER=appuser
Environment=POSTGRES_DB=appdb

[Service]
Restart=always
TimeoutStartSec=60

[Install]
WantedBy=multi-user.target
```

**File: database/postgres-data.volume**

```ini
[Unit]
Description=PostgreSQL Data Volume

[Volume]
User=999
Group=999
Label=app=postgres
```

**FetchIt config.yaml:**

```yaml
targetConfigs:
- name: databases
  url: https://github.com/your-org/containers
  branch: main

  quadlet:
  - name: postgres
    targetPath: database/
    glob: "postgres.*"
    schedule: "0 */6 * * *"  # Every 6 hours
    root: true
    enable: true
    restart: false  # Don't auto-restart database
```

### Example 3: Multi-Tier Application

**File: app/frontend.container**

```ini
[Unit]
Description=Frontend Web Application
After=app.network backend.service

[Container]
Image=docker.io/your-org/frontend:latest
PublishPort=3000:3000
Network=app.network
Environment=BACKEND_URL=http://backend:8000
Environment=NODE_ENV=production

[Service]
Restart=always

[Install]
WantedBy=multi-user.target
```

**File: app/backend.container**

```ini
[Unit]
Description=Backend API Service
After=app.network database.service

[Container]
Image=docker.io/your-org/backend:latest
PublishPort=8000:8000
Network=app.network
Volume=app-data.volume:/data:Z
Environment=DATABASE_URL=postgresql://db:5432/appdb
Environment=SECRET_KEY=change-me

[Service]
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

**File: app/app.network**

```ini
[Unit]
Description=Application Network

[Network]
Subnet=10.90.0.0/16
Gateway=10.90.0.1
Label=app=myapp
Label=tier=backend
```

**File: app/app-data.volume**

```ini
[Unit]
Description=Application Data Volume

[Volume]
User=1000
Group=1000
```

**FetchIt config.yaml:**

```yaml
targetConfigs:
- name: myapp
  url: https://github.com/your-org/myapp-deploy
  branch: production

  quadlet:
  - name: infrastructure
    targetPath: app/
    glob: "*.{network,volume}"
    schedule: "*/15 * * * *"
    root: true
    enable: true
    restart: false

  - name: services
    targetPath: app/
    glob: "*.container"
    schedule: "*/10 * * * *"
    root: true
    enable: true
    restart: true
```

---

## Best Practices

### 1. Use Descriptive Names

**Good:**
```
web-frontend.container
api-backend.container
postgres-db.container
```

**Bad:**
```
app1.container
service.container
test.container
```

### 2. Organize by Environment

```
configs/
├── production/
│   ├── web.container
│   └── db.container
├── staging/
│   ├── web.container
│   └── db.container
└── development/
    ├── web.container
    └── db.container
```

### 3. Use Glob Patterns Strategically

```yaml
# Separate critical services from non-critical
quadlet:
- name: critical-services
  glob: "critical-*.container"
  restart: false  # Manual restart for critical services

- name: worker-services
  glob: "worker-*.container"
  restart: true  # Auto-restart workers
```

### 4. Set Appropriate Schedules

```yaml
# Frequent checks for development
schedule: "*/2 * * * *"  # Every 2 minutes

# Balanced for production
schedule: "*/10 * * * *"  # Every 10 minutes

# Infrequent for stable services
schedule: "0 */6 * * *"  # Every 6 hours
```

### 5. Use Version Tags, Not Latest

**Good:**
```ini
Image=docker.io/nginx:1.25.3
```

**Bad:**
```ini
Image=docker.io/nginx:latest
```

### 6. Define Resource Limits

```ini
[Container]
Image=docker.io/app:v1.0
Memory=512M
MemorySwap=1G
CPUQuota=50%
```

### 7. Implement Health Checks

```ini
[Container]
Image=docker.io/app:v1.0
HealthCmd=/usr/bin/curl -f http://localhost:8080/health || exit 1
HealthInterval=30s
HealthRetries=3
HealthStartPeriod=40s
HealthTimeout=10s
```

### 8. Use Separate Configs for Different Lifecycle Policies

```yaml
targetConfigs:
- name: production-db
  quadlet:
  - name: databases
    restart: false  # Never auto-restart
    enable: true

- name: production-web
  quadlet:
  - name: web-services
    restart: true  # Auto-restart on updates
    enable: true
```

---

## Troubleshooting

### Issue: Service fails to start

**Symptoms:**
```bash
systemctl --user status nginx.service
# Status: Failed
```

**Diagnosis:**

```bash
# Check service logs
journalctl --user -u nginx.service -n 50

# Check container logs
podman logs nginx

# Common causes:
# - Image pull failure
# - Port already in use
# - Volume mount permission denied
# - Invalid environment variables
```

**Solutions:**

```bash
# Manually pull image
podman pull docker.io/nginx:latest

# Check port availability
ss -tulpn | grep 8080

# Fix volume permissions
chmod 755 ~/.local/share/containers/storage/volumes/
```

### Issue: Quadlet file not detected

**Symptoms:**
- File copied to systemd directory
- No corresponding service created

**Diagnosis:**

```bash
# Check file permissions
ls -la ~/.config/containers/systemd/

# Check file extension
file ~/.config/containers/systemd/nginx.container

# Manually reload daemon
systemctl --user daemon-reload

# Check for errors
journalctl --user -xe
```

**Solutions:**

```bash
# Ensure correct file extension
mv nginx.cont nginx.container

# Fix permissions
chmod 644 ~/.config/containers/systemd/nginx.container

# Reload daemon
systemctl --user daemon-reload
```

### Issue: Permission denied errors

**Symptoms:**
```
Error: mkdir /etc/containers/systemd: permission denied
```

**Solutions:**

```bash
# For root mode, ensure FetchIt runs as root
sudo podman run ... quay.io/fetchit/fetchit:latest

# For user mode, use root=false
# Check $HOME is set
echo $HOME

# Check directory writable
ls -ld ~/.config/containers/systemd/
```

### Issue: XDG_RUNTIME_DIR not set (user mode)

**Symptoms:**
```
Error: XDG_RUNTIME_DIR not set
```

**Solutions:**

```bash
# Set environment variable
export XDG_RUNTIME_DIR=/run/user/$(id -u)

# Add to FetchIt run command
-e XDG_RUNTIME_DIR=/run/user/$(id -u)
```

### Issue: Service doesn't persist after logout

**Symptoms:**
- Service stops when SSH session ends
- Service not running after reboot

**Solutions:**

```bash
# Enable lingering
loginctl enable-linger $USER

# Verify lingering enabled
loginctl show-user $USER | grep Linger
# Output: Linger=yes

# Check service enabled
systemctl --user is-enabled nginx.service
```

### Issue: Changes not applied by FetchIt

**Symptoms:**
- Git commit pushed
- FetchIt logs show no activity
- Old configuration still deployed

**Diagnosis:**

```bash
# Check FetchIt logs
podman logs fetchit

# Check schedule timing
# If schedule is "*/10 * * * *", wait up to 10 minutes

# Check git repository accessible
podman exec -it fetchit git ls-remote <repo-url>
```

**Solutions:**

```bash
# Verify config.yaml syntax
cat ~/.fetchit/config.yaml

# Restart FetchIt
podman restart fetchit

# Check branch is correct
podman exec -it fetchit git -C /opt/<repo> branch
```

---

## Migration Guide

### Migrating from Systemd Method to Quadlet

See [migration.md](./migration.md) for detailed migration instructions.

**Quick Summary:**

**Before (Systemd method):**

```yaml
systemd:
- name: nginx
  targetPath: systemd/
  glob: "*.service"
  root: true
  enable: true
```

**After (Quadlet method):**

```yaml
quadlet:
- name: nginx
  targetPath: quadlet/
  glob: "*.container"
  root: true
  enable: true
  restart: true
```

**Key Differences:**

1. File format changes from `.service` to `.container`
2. Quadlet uses `[Container]` section instead of `ExecStart=podman run`
3. Quadlet auto-generates systemd units (no manual unit creation)
4. Quadlet supports `.volume` and `.network` files natively

---

## Additional Resources

- **Podman Quadlet Documentation**: https://docs.podman.io/en/latest/markdown/podman-systemd.unit.5.html
- **FetchIt GitHub**: https://github.com/containers/fetchit
- **FetchIt Examples**: https://github.com/containers/fetchit/tree/main/examples/quadlet
- **systemd Documentation**: https://www.freedesktop.org/software/systemd/man/systemd.unit.html

---

## Support

For questions and issues:

- **GitHub Issues**: https://github.com/containers/fetchit/issues
- **Discussions**: https://github.com/containers/fetchit/discussions
- **IRC**: #fetchit on libera.chat

---

**Last Updated**: 2025-10-15
**FetchIt Version**: v0.0.0+ (with Quadlet support)
**Podman Version**: 4.9.4+
