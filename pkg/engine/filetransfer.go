package engine

import (
	"context"
	"path/filepath"

	"k8s.io/klog/v2"
)

func fileTransferPodman(ctx context.Context, mo *FileMountOptions, prev *string, dest string) error {
	if prev != nil {
		pathToRemove := filepath.Join(dest, filepath.Base(*prev))
		s := generateSpecRemove(mo.Method, filepath.Base(pathToRemove), pathToRemove, dest, mo.Target)
		createResponse, err := createAndStartContainer(mo.Conn, s)
		if err != nil {
			return err
		}

		err = waitAndRemoveContainer(mo.Conn, createResponse.ID)
		if err != nil {
			return err
		}
	}

	if mo.Path == deleteFile {
		return nil
	}

	klog.Infof("Deploying file(s) %s", mo.Path)

	file := filepath.Base(mo.Path)

	source := filepath.Join("/opt", mo.Path)
	copyFile := (source + " " + dest)

	s := generateSpec(mo.Method, file, copyFile, dest, mo.Target)
	createResponse, err := createAndStartContainer(mo.Conn, s)
	if err != nil {
		return err
	}

	// Wait for the container to exit
	return waitAndRemoveContainer(mo.Conn, createResponse.ID)
}
