package engine

import (
	"context"

	"github.com/containers/podman/v4/libpod/define"
	"github.com/containers/podman/v4/pkg/bindings/containers"
	"github.com/containers/podman/v4/pkg/domain/entities"
	"github.com/containers/podman/v4/pkg/specgen"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/redhat-et/harpoon/pkg/engine/api"

	"k8s.io/klog/v2"
)

const stopped = define.ContainerStateStopped

func generateSpec(method, file, copyFile, dest string, target *api.Target) *specgen.SpecGenerator {
	s := specgen.NewSpecGenerator(harpoonImage, false)
	s.Name = method + "-" + target.Name + "-" + file
	s.Privileged = true
	s.PidNS = specgen.Namespace{
		NSMode: "host",
		Value:  "",
	}
	s.Command = []string{"sh", "-c", "cp " + copyFile}
	s.Mounts = []specs.Mount{{Source: dest, Destination: dest, Type: "bind", Options: []string{"rw"}}}
	s.Volumes = []*specgen.NamedVolume{{Name: harpoonVolume, Dest: "/opt", Options: []string{"ro"}}}
	return s
}

func generateSpecRemove(method, file, pathToRemove, dest string, target *api.Target) *specgen.SpecGenerator {
	s := specgen.NewSpecGenerator(harpoonImage, false)
	s.Name = method + "-" + target.Name + "-" + file
	s.Privileged = true
	s.PidNS = specgen.Namespace{
		NSMode: "host",
		Value:  "",
	}
	s.Command = []string{"sh", "-c", "rm " + pathToRemove}
	s.Mounts = []specs.Mount{{Source: dest, Destination: dest, Type: "bind", Options: []string{"rw"}}}
	s.Volumes = []*specgen.NamedVolume{{Name: harpoonVolume, Dest: "/opt", Options: []string{"ro"}}}
	return s
}

func createAndStartContainer(conn context.Context, s *specgen.SpecGenerator) (entities.ContainerCreateResponse, error) {
	createResponse, err := containers.CreateWithSpec(conn, s, nil)
	if err != nil {
		return createResponse, err
	}

	if err := containers.Start(conn, createResponse.ID, nil); err != nil {
		return createResponse, err
	}
	klog.Infof("Container %s created.", s.Name)

	return createResponse, nil
}

func waitAndRemoveContainer(conn context.Context, ID string) error {
	_, err := containers.Wait(conn, ID, new(containers.WaitOptions).WithCondition([]define.ContainerStatus{stopped}))
	if err != nil {
		return err
	}

	_, err = containers.Remove(conn, ID, new(containers.RemoveOptions).WithForce(true))
	if err != nil {
		// There's a podman bug somewhere that's causing this
		if err.Error() == "unexpected end of JSON input" {
			return nil
		}
		return err
	}

	return nil
}
