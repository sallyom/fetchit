package engine

import (
	"context"
	"time"

	"github.com/containers/podman/v4/pkg/bindings/containers"
	"github.com/containers/podman/v4/pkg/specgen"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/opencontainers/runtime-spec/specs-go"

	"k8s.io/klog/v2"
)

const ansibleMethod = "ansible"

// Ansible to place and run ansible playbooks
type Ansible struct {
	DefaultMethod `mapstructure:",squash"`
	// SshDirectory for ansible to connect to host
	SshDirectory string `mapstructure:"sshDirectory"`
}

func (a *Ansible) Type() string {
	return ansibleMethod
}

func (ans *Ansible) Process(ctx, conn context.Context, PAT string, skew int) {
	time.Sleep(time.Duration(skew) * time.Millisecond)
	target := ans.Target()
	target.mu.Lock()
	defer target.mu.Unlock()

	tag := []string{"yaml", "yml"}
	if ans.initialRun {
		err := getClone(target, PAT)
		if err != nil {
			klog.Errorf("Failed to clone repo at %s for target %s: %v", target.url, target.Name, err)
			return
		}
	}

	err := ans.currentToLatest(ctx, conn, target, &tag)
	if err != nil {
		klog.Errorf("Error moving current to latest: %v", err)
		return
	}

	ans.initialRun = false
}

func (ans *Ansible) MethodEngine(ctx context.Context, conn context.Context, change *object.Change, path string) error {
	return ans.ansiblePodman(ctx, conn, path)
}

func (ans *Ansible) Apply(ctx, conn context.Context, target *Target, currentState, desiredState plumbing.Hash, targetPath string, tags *[]string) error {
	changeMap, err := applyChanges(ctx, target, currentState, desiredState, targetPath, tags)
	if err != nil {
		return err
	}
	if err := ans.runChangesConcurrent(ctx, conn, changeMap); err != nil {
		return err
	}
	return nil
}

func (ans *Ansible) ansiblePodman(ctx, conn context.Context, path string) error {
	// TODO: add logic to remove
	if path == deleteFile {
		return nil
	}
	klog.Infof("Deploying Ansible playbook %s\n", path)

	copyFile := ("/opt/" + path)
	sshImage := "quay.io/fetchit/fetchit-ansible:latest"

	klog.Infof("Identifying if fetchit-ansible image exists locally")
	if err := detectOrFetchImage(conn, sshImage, true); err != nil {
		return err
	}

	s := specgen.NewSpecGenerator(sshImage, false)
	s.Name = "ansible" + "-" + ans.Name
	s.Privileged = true
	s.PidNS = specgen.Namespace{
		NSMode: "host",
		Value:  "",
	}

	// TODO: Remove rcook entries
	s.Command = []string{"sh", "-c", "/usr/bin/ansible-playbook -e ansible_connection=ssh " + copyFile}
	s.Mounts = []specs.Mount{{Source: ans.SshDirectory, Destination: "/root/.ssh", Type: "bind", Options: []string{"rw"}}}
	s.Volumes = []*specgen.NamedVolume{{Name: fetchitVolume, Dest: "/opt", Options: []string{"ro"}}}
	s.NetNS = specgen.Namespace{
		NSMode: "host",
		Value:  "",
	}
	createResponse, err := containers.CreateWithSpec(conn, s, nil)
	if err != nil {
		return err
	}
	klog.Infof("Container created.")
	if err := containers.Start(conn, createResponse.ID, nil); err != nil {
		return err
	}
	// Wait for the container to exit
	err = waitAndRemoveContainer(conn, createResponse.ID)
	if err != nil {
		return err
	}
	klog.Infof("Container started....Requeuing")
	return nil
}
