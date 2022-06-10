package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/containers/fetchit/pkg/engine/utils"
	"github.com/go-co-op/gocron"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"k8s.io/klog/v2"
)

type Method interface {
	Type() string
	GetName() string
	Target() *Target
	SetTarget(*Target)
	SetInitialRun(bool)
	SchedInfo() SchedInfo
	Process(ctx context.Context, conn context.Context, PAT string, skew int)
	Apply(ctx context.Context, conn context.Context, target *Target, currentState plumbing.Hash, desiredState plumbing.Hash, targetPath string, tags *[]string) error
	MethodEngine(ctx context.Context, conn context.Context, change *object.Change, path string) error
}

type DefaultMethod struct {
	// Name must be unique within target Raw methods
	Name string `mapstructure:"name"`
	// Schedule is how often to check for git updates and/or restart the fetchit service
	// Must be valid cron expression
	Schedule string `mapstructure:"schedule"`
	// Number of seconds to skew the schedule by
	Skew *int `mapstructure:"skew"`
	// Where in the git repository to fetch a file or directory (to fetch all files in directory)
	TargetPath string `mapstructure:"targetPath"`
	// initialRun is set by fetchit
	initialRun bool
	target     *Target
}

func (m *DefaultMethod) Type() string {
	return "default"
}

func (m *DefaultMethod) GetName() string {
	return m.Name
}

func (m *DefaultMethod) SchedInfo() SchedInfo {
	return SchedInfo{
		schedule: m.Schedule,
		skew:     m.Skew,
	}
}

func (m *DefaultMethod) Target() *Target {
	return m.target
}

func (m *DefaultMethod) SetTarget(t *Target) {
	m.target = t
}

func (m *DefaultMethod) SetInitialRun(b bool) {
	m.initialRun = b
}

func (m *DefaultMethod) currentToLatest(ctx, conn context.Context, target *Target, tag *[]string) error {
	latest, err := getLatest(target)
	if err != nil {
		return fmt.Errorf("Failed to get latest commit: %v", err)
	}

	current, err := getCurrent(target, systemdMethod, m.Name)
	if err != nil {
		return fmt.Errorf("Failed to get current commit: %v", err)
	}

	if latest != current {
		err = m.Apply(ctx, conn, target, current, latest, m.TargetPath, tag)
		if err != nil {
			return fmt.Errorf("Failed to apply changes: %v", err)
		}

		updateCurrent(ctx, target, latest, systemdMethod, m.Name)
		klog.Infof("Moved systemd %s from %s to %s for target %s", m.Name, current, latest, target.Name)
	} else {
		klog.Infof("No changes applied to target %s this run, %s currently at %s", target.Name, m.Type(), current)
	}

	return nil
}

func (m *DefaultMethod) Process(ctx context.Context, conn context.Context, PAT string, skew int) {
	return
}

func (m *DefaultMethod) Apply(ctx context.Context, conn context.Context, target *Target, currentState plumbing.Hash, desiredState plumbing.Hash, targetPath string, tags *[]string) error {
	return nil
}

func (m *DefaultMethod) MethodEngine(ctx context.Context, conn context.Context, change *object.Change, path string) error {
	return nil
}

func (m *DefaultMethod) runChangesConcurrent(ctx context.Context, conn context.Context, changeMap map[*object.Change]string) error {
	ch := make(chan error)
	for change, changePath := range changeMap {
		go func(ch chan<- error, changePath string, change *object.Change) {
			if err := m.MethodEngine(ctx, conn, change, changePath); err != nil {
				ch <- utils.WrapErr(err, "error running engine method for change from: %s to %s", change.From.Name, change.To.Name)
			}
			ch <- nil
		}(ch, changePath, change)
	}
	for range changeMap {
		err := <-ch
		if err != nil {
			return err
		}
	}
	return nil
}

// FetchitConfig requires necessary objects to process targets
type FetchitConfig struct {
	TargetConfigs []*TargetConfig `mapstructure:"targetConfigs"`
	ConfigReload  *ConfigReload   `mapstructure:"configReload"`
	PAT           string          `mapstructure:"pat"`
	volume        string          `mapstructure:"volume"`
	conn          context.Context
	scheduler     *gocron.Scheduler
}

type TargetConfig struct {
	Name         string                   `mapstructure:"name"`
	Url          string                   `mapstructure:"url"`
	Branch       string                   `mapstructure:"branch"`
	Clean        *Clean                   `mapstructure:"clean"`
	Methods      []map[string]interface{} `mapstructure:"methods"`
	configReload *ConfigReload
	mu           sync.Mutex
}

type Target struct {
	Name         string
	url          string
	branch       string
	configReload *ConfigReload
	mu           sync.Mutex
}

type SchedInfo struct {
	schedule string
	skew     *int
}
