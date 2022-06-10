package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/containers/fetchit/pkg/engine/utils"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"k8s.io/klog/v2"
)

type CommonMethod struct {
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

type SchedInfo struct {
	schedule string
	skew     *int
}

func (c *CommonMethod) SchedInfo() SchedInfo {
	return SchedInfo{
		schedule: c.Schedule,
		skew:     c.Skew,
	}
}

func (c *CommonMethod) SetTarget(t *Target) {
	c.target = t
}

func (c *CommonMethod) SetInitialRun(b bool) {
	c.initialRun = b
}

func applyChanges(ctx context.Context, target *Target, targetPath string, currentState, desiredState plumbing.Hash, tags *[]string) (map[*object.Change]string, error) {
	if desiredState.IsZero() {
		return nil, errors.New("Cannot run Apply if desired state is empty")
	}
	directory := filepath.Base(target.url)

	currentTree, err := getTreeFromHash(directory, currentState)
	if err != nil {
		return nil, utils.WrapErr(err, "Error getting tree from hash %s", currentState)
	}

	desiredTree, err := getTreeFromHash(directory, desiredState)
	if err != nil {
		return nil, utils.WrapErr(err, "Error getting tree from hash %s", desiredState)
	}

	changeMap, err := getFilteredChangeMap(directory, targetPath, currentTree, desiredTree, tags)
	if err != nil {
		return nil, utils.WrapErr(err, "Error getting filtered change map from %s to %s", currentState, desiredState)
	}

	return changeMap, nil
}

func currentToLatest(ctx, conn context.Context, m Method, tag *[]string) error {
	target := m.Target()
	latest, err := getLatest(target)
	if err != nil {
		return fmt.Errorf("Failed to get latest commit: %v", err)
	}

	current, err := getCurrent(target, m.Type(), m.Name())
	if err != nil {
		return fmt.Errorf("Failed to get current commit: %v", err)
	}

	if latest != current {
		err = m.Apply(ctx, conn, current, latest, tag)
		if err != nil {
			return fmt.Errorf("Failed to apply changes: %v", err)
		}

		updateCurrent(ctx, target, latest, m.Type(), m.Name())
		klog.Infof("Moved %s %s from %s to %s for target %s", m.Type(), m.Name(), current, latest, target.Name)
	} else {
		klog.Infof("No changes applied to target %s this run, %s currently at %s", target.Name, m.Type(), current)
	}

	return nil
}

func runChangesConcurrent(ctx, conn context.Context, m Method, changeMap map[*object.Change]string) error {
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
