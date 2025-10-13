package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/containers/fetchit/pkg/engine/utils"
	"github.com/containers/podman/v4/libpod/define"
	"github.com/containers/podman/v4/pkg/specgen"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	quadletMethod       = "quadlet"
	quadletImage        = "quay.io/fetchit/fetchit-systemd:latest"
	quadletPathRoot     = "/etc/containers/systemd"
	quadletPathUserBase = ".config/containers/systemd"
)

// Quadlet manages Podman Quadlet unit files (.container, .volume, .network) via git
// Quadlet is the modern replacement for 'podman generate systemd'
type Quadlet struct {
	CommonMethod `mapstructure:",squash"`
	// If true, will place unit files in /etc/containers/systemd/ (system-wide, requires root)
	// If false (default), will place unit files in ~/.config/containers/systemd/ (user-level, rootless)
	Root bool `mapstructure:"root"`
	// If true, will enable and start systemd services from fetched Quadlet files
	// If false (default), will place Quadlet file(s) and trigger daemon-reload only
	Enable bool `mapstructure:"enable"`
	// If true, will restart services when Quadlet files are updated
	// Implies Enable=true, will override Enable=false
	Restart bool `mapstructure:"restart"`
}

func (q *Quadlet) GetKind() string {
	return quadletMethod
}

func (q *Quadlet) Process(ctx, conn context.Context, skew int) {
	target := q.GetTarget()
	time.Sleep(time.Duration(skew) * time.Millisecond)
	target.mu.Lock()
	defer target.mu.Unlock()

	// Quadlet supports .container, .volume, .network files
	tag := []string{".container", ".volume", ".network"}

	if q.Restart {
		q.Enable = true
	}

	if q.initialRun {
		err := getRepo(target)
		if err != nil {
			logger.Errorf("Failed to clone repository %s: %v", target.url, err)
			return
		}

		err = zeroToCurrent(ctx, conn, q, target, &tag)
		if err != nil {
			logger.Errorf("Error moving to current: %v", err)
			return
		}
	}

	err := currentToLatest(ctx, conn, q, target, &tag)
	if err != nil {
		logger.Errorf("Error moving current to latest: %v", err)
		return
	}

	q.initialRun = false
}

func (q *Quadlet) Apply(ctx, conn context.Context, currentState, desiredState plumbing.Hash, tags *[]string) error {
	changeMap, err := applyChanges(ctx, q.GetTarget(), q.GetTargetPath(), q.Glob, currentState, desiredState, tags)
	if err != nil {
		return err
	}
	if err := runChanges(ctx, conn, q, changeMap); err != nil {
		return err
	}
	return nil
}

func (q *Quadlet) MethodEngine(ctx context.Context, conn context.Context, change *object.Change, path string) error {
	var changeType string = "unknown"
	var curr *string = nil
	var prev *string = nil

	if change != nil {
		if change.From.Name != "" {
			prev = &change.From.Name
		}
		if change.To.Name != "" {
			curr = &change.To.Name
		}
		if change.From.Name == "" && change.To.Name != "" {
			changeType = "create"
		}
		if change.From.Name != "" && change.To.Name != "" {
			if change.From.Name == change.To.Name {
				changeType = "update"
			} else {
				changeType = "rename"
			}
		}
		if change.From.Name != "" && change.To.Name == "" {
			changeType = "delete"
		}
	}

	// Determine destination directory
	dest, err := q.destDir()
	if err != nil {
		return err
	}

	if change != nil {
		q.initialRun = true
	}

	return q.quadletPodman(ctx, conn, path, dest, prev, curr, &changeType)
}

// destDir calculates the destination directory for Quadlet files based on Root setting
func (q *Quadlet) destDir() (string, error) {
	if q.Root {
		return quadletPathRoot, nil
	}

	nonRootHomeDir := os.Getenv("HOME")
	if nonRootHomeDir == "" {
		return "", fmt.Errorf("Could not determine $HOME for host, must set $HOME on host machine for non-root quadlet method")
	}

	return filepath.Join(nonRootHomeDir, quadletPathUserBase), nil
}

// serviceNameFromFile derives the systemd service name from a Quadlet file name
// Example: nginx.container -> nginx.service
func (q *Quadlet) serviceNameFromFile(filename string) string {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]
	return name + ".service"
}

// quadletPodman handles deployment of Quadlet files and systemd service management
func (q *Quadlet) quadletPodman(ctx context.Context, conn context.Context, path, dest string, prev *string, curr *string, changeType *string) error {
	logger.Infof("Deploying Quadlet file(s) %s", path)

	if q.initialRun {
		// Use FileTransfer to copy Quadlet files to destination
		ft := &FileTransfer{
			CommonMethod: CommonMethod{
				Name: q.Name,
			},
		}
		if err := ft.fileTransferPodman(ctx, conn, path, dest, prev); err != nil {
			return utils.WrapErr(err, "Error deploying Quadlet %s file(s), Path: %s", q.Name, q.TargetPath)
		}

		// Always run daemon-reload after deploying Quadlet files
		// This triggers the Quadlet generator to process .container, .volume, .network files
		if err := q.systemctlDaemonReload(conn, dest); err != nil {
			return utils.WrapErr(err, "Error running systemctl daemon-reload for Quadlet %s", q.Name)
		}
	}

	if !q.Enable {
		logger.Infof("Quadlet target %s successfully processed", q.Name)
		return nil
	}

	// Handle service management based on change type
	if *changeType == "create" {
		serviceName := q.serviceNameFromFile(*curr)
		return q.systemctlManageService(conn, "enable", dest, serviceName)
	}

	if *changeType == "update" {
		serviceName := q.serviceNameFromFile(*curr)
		if q.Restart {
			return q.systemctlManageService(conn, "restart", dest, serviceName)
		} else {
			return q.systemctlManageService(conn, "enable", dest, serviceName)
		}
	}

	if *changeType == "rename" {
		// Stop old service, enable new service
		if prev != nil {
			prevService := q.serviceNameFromFile(*prev)
			if err := q.systemctlManageService(conn, "stop", dest, prevService); err != nil {
				return err
			}
		}
		if curr != nil {
			currService := q.serviceNameFromFile(*curr)
			return q.systemctlManageService(conn, "enable", dest, currService)
		}
	}

	if *changeType == "delete" {
		if prev != nil {
			prevService := q.serviceNameFromFile(*prev)
			return q.systemctlManageService(conn, "stop", dest, prevService)
		}
	}

	logger.Infof("Quadlet target %s %s not processed", q.Name, *changeType)
	return nil
}

// systemctlDaemonReload runs 'systemctl daemon-reload' to trigger Quadlet generator
func (q *Quadlet) systemctlDaemonReload(conn context.Context, dest string) error {
	logger.Infof("Quadlet target: %s, running systemctl daemon-reload", q.Name)

	if err := detectOrFetchImage(conn, quadletImage, false); err != nil {
		return err
	}

	s := specgen.NewSpecGenerator(quadletImage, false)
	runMounttmp := "/run"
	runMountsd := "/run/systemd"
	runMountc := "/sys/fs/cgroup"
	xdg := ""

	if !q.Root {
		xdg = os.Getenv("XDG_RUNTIME_DIR")
		if xdg == "" {
			xdg = "/run/user/1000"
		}
		runMountsd = xdg + "/systemd"
		runMounttmp = xdg
	}

	s.Privileged = true
	s.PidNS = specgen.Namespace{
		NSMode: "host",
		Value:  "",
	}
	s.Mounts = []specs.Mount{
		{Source: dest, Destination: dest, Type: define.TypeBind, Options: []string{"rw"}},
		{Source: runMounttmp, Destination: runMounttmp, Type: define.TypeTmpfs, Options: []string{"rw"}},
		{Source: runMountc, Destination: runMountc, Type: define.TypeBind, Options: []string{"ro"}},
		{Source: runMountsd, Destination: runMountsd, Type: define.TypeBind, Options: []string{"rw"}},
	}
	s.Name = "quadlet-daemon-reload-" + q.Name

	envMap := make(map[string]string)
	envMap["ROOT"] = strconv.FormatBool(q.Root)
	envMap["SERVICE"] = ""
	envMap["ACTION"] = "reload"
	envMap["HOME"] = os.Getenv("HOME")
	if !q.Root {
		envMap["XDG_RUNTIME_DIR"] = xdg
	}
	s.Env = envMap

	createResponse, err := createAndStartContainer(conn, s)
	if err != nil {
		return err
	}

	err = waitAndRemoveContainer(conn, createResponse.ID)
	if err != nil {
		return err
	}

	logger.Infof("Quadlet target %s daemon-reload complete", q.Name)
	return nil
}

// systemctlManageService manages systemd services (enable, start, restart, stop)
func (q *Quadlet) systemctlManageService(conn context.Context, action, dest, service string) error {
	logger.Infof("Quadlet target: %s, running systemctl %s %s", q.Name, action, service)

	if err := detectOrFetchImage(conn, quadletImage, false); err != nil {
		return err
	}

	s := specgen.NewSpecGenerator(quadletImage, false)
	runMounttmp := "/run"
	runMountsd := "/run/systemd"
	runMountc := "/sys/fs/cgroup"
	xdg := ""

	if !q.Root {
		xdg = os.Getenv("XDG_RUNTIME_DIR")
		if xdg == "" {
			xdg = "/run/user/1000"
		}
		runMountsd = xdg + "/systemd"
		runMounttmp = xdg
	}

	s.Privileged = true
	s.PidNS = specgen.Namespace{
		NSMode: "host",
		Value:  "",
	}
	s.Mounts = []specs.Mount{
		{Source: dest, Destination: dest, Type: define.TypeBind, Options: []string{"rw"}},
		{Source: runMounttmp, Destination: runMounttmp, Type: define.TypeTmpfs, Options: []string{"rw"}},
		{Source: runMountc, Destination: runMountc, Type: define.TypeBind, Options: []string{"ro"}},
		{Source: runMountsd, Destination: runMountsd, Type: define.TypeBind, Options: []string{"rw"}},
	}
	s.Name = "quadlet-" + action + "-" + service + "-" + q.Name

	envMap := make(map[string]string)
	envMap["ROOT"] = strconv.FormatBool(q.Root)
	envMap["SERVICE"] = service
	envMap["ACTION"] = action
	envMap["HOME"] = os.Getenv("HOME")
	if !q.Root {
		envMap["XDG_RUNTIME_DIR"] = xdg
	}
	s.Env = envMap

	createResponse, err := createAndStartContainer(conn, s)
	if err != nil {
		return err
	}

	err = waitAndRemoveContainer(conn, createResponse.ID)
	if err != nil {
		return err
	}

	logger.Infof("Quadlet target %s-%s %s complete", q.Name, action, service)
	return nil
}
