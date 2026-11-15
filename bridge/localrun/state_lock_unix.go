//go:build unix

package localrun

import (
	"os"

	"golang.org/x/sys/unix"
)

/**
 * @brief Make atomically renamed or deleted directory entries durable before returning success.
 * @param root Private state directory.
 * @return Directory open or fsync error.
 */
func syncStateDirectory(root string) error {
	file, err := os.Open(root)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}

/**
 * @brief Acquire a process-lifetime nonblocking writer lease, released by closing the file.
 * @param file Open state-root lock file.
 * @return OS locking failure when another process owns this state root.
 */
func lockStateFile(file *os.File) error { return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB) }
