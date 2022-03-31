package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/containers/podman/v4/pkg/bindings/images"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/spf13/viper"

	"k8s.io/klog/v2"
)

func isLocalConfig(config *HarpoonConfig, v *viper.Viper) bool {
	if _, err := os.Stat(DefaultConfigPath); err != nil {
		klog.Infof("Local config file not found: %v", err)
		return false
	}
	if err := v.ReadInConfig(); err == nil {
		if err := v.Unmarshal(&config); err != nil {
			klog.Info("Error with unmarshal of existing config file: %v", err)
			return false
		}
	}
	return true
}

func downloadUpdateConfigFile(urlStr string) error {
	_, err := url.Parse(urlStr)
	if err != nil {
		// TODO: reset instead
		return fmt.Errorf("unable to parse config file url %s: %v", urlStr, err)
	}
	err = os.MkdirAll(filepath.Dir(DefaultConfigNew), 0600)
	if err != nil {
		return err
	}
	file, err := os.Create(DefaultConfigNew)
	if err != nil {
		return err
	}
	client := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}
	resp, err := client.Get(urlStr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}
	defer file.Close()
	klog.Infof("Config from %s placed at %s", urlStr, DefaultConfigNew)
	return nil
}

// compareFiles returns true if either cannot read oldFile or newFile is not identical
// if newFile cannot be read or is identical to oldFile, returns false
// otherwise, returns error
func compareFiles(newPath, oldPath string) (bool, error) {
	replace := false
	oldFile, oldFileErr := ioutil.ReadFile(oldPath)
	if oldFileErr != nil {
		klog.Warningf("Error reading config file %s", oldPath)
		replace = true
	}
	newFile, newFileErr := ioutil.ReadFile(newPath)
	if newFileErr != nil {
		replace = false
		klog.Warningf("Error reading updated config file, continuing without config updates")

	}
	if oldFileErr != nil && newFileErr != nil {
		klog.Warningf("%v, %v", oldFileErr, newFileErr)
		return false, fmt.Errorf("error fetching config updates: %v, %v", oldFileErr, newFileErr)
	}
	if oldFileErr == nil && newFileErr == nil {
		if !bytes.Equal(oldFile, newFile) || replace {
			return true, nil
		}
	}
	return replace, nil
}

func getChangeString(change *object.Change) (*string, error) {
	if change != nil {
		_, to, err := change.Files()
		if err != nil {
			return nil, err
		}
		if to != nil {
			s, err := to.Contents()
			if err != nil {
				return nil, err
			}
			return &s, nil
		}
	}
	return nil, nil
}

func checkTag(tags *[]string, name string) bool {
	if tags == nil {
		return true
	}
	for _, tag := range *tags {
		if strings.HasSuffix(name, tag) {
			return true
		}
	}
	return false
}

func getTree(r *git.Repository, oldCommit *object.Commit) (*object.Tree, *object.Commit, error) {
	if oldCommit != nil {
		// ... retrieve the tree from the commit
		tree, err := oldCommit.Tree()
		if err != nil {
			return nil, nil, fmt.Errorf("error when retrieving tree: %s", err)
		}
		return tree, nil, nil
	}
	var newCommit *object.Commit
	ref, err := r.Head()
	if err != nil {
		return nil, nil, fmt.Errorf("error when retrieving head: %s", err)
	}
	// ... retrieving the commit object
	newCommit, err = r.CommitObject(ref.Hash())
	if err != nil {
		return nil, nil, fmt.Errorf("error when retrieving commit: %s", err)
	}

	// ... retrieve the tree from the commit
	tree, err := newCommit.Tree()
	if err != nil {
		return nil, nil, fmt.Errorf("error when retrieving tree: %s", err)
	}
	return tree, newCommit, nil
}

// CopyFile takes contents of file and places at newPath
// CopyFile(from, to)
func CopyFile(toCopy string, newPath string) error {
	klog.Infof("File %s will be copied to %s", toCopy, newPath)
	configBytes, err := os.ReadFile(toCopy)
	if err != nil {
		return fmt.Errorf("could not read config file: %v", err)
	}
	newPathAlreadyExists := false
	existingFile, err := os.Open(newPath)
	if err == nil {
		newPathAlreadyExists = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// overwrite
	if newPathAlreadyExists {
		existingFile.Truncate(0)
		existingFile.Seek(0, 0)
	}

	if err := os.WriteFile(newPath, configBytes, os.ModePerm); err != nil {
		return fmt.Errorf("could not copy %s to path %s: %v", toCopy, newPath, err)
	}
	return nil
}

func FetchImage(conn context.Context, image string, force bool) error {
	present, err := images.Exists(conn, image, nil)
	if err != nil {
		return err
	}

	if !present || force {
		klog.Infof("Pulling image %s", image)
		_, err = images.Pull(conn, image, nil)
		if err != nil {
			return err
		}
	}
	return nil
}
