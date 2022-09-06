package engine

import (
	"os"
	"path/filepath"
)

var defaultSSHPath = filepath.Join("/opt", "mount", ".ssh", "id_rsa")

// Basic type needed for ssh authentication
type GitAuth struct {
	SSH bool
	// 	username string
	// 	password string
	// 	pat      string
}

func checkForPrivateKey() error {
	if _, err := os.Stat(defaultSSHPath); err != nil {
		return err
	}
	return nil
}
