//go:build windows

package localrun

import (
	"os"

	"golang.org/x/sys/windows"
)

/**
 * @brief Rely on Windows rename metadata handling after the file itself has been flushed.
 * @param _ State directory; Go does not support syncing directory handles on Windows.
 * @return Always nil.
 */
func syncStateDirectory(_ string) error { return nil }

/**
 * @brief Acquire a process-lifetime nonblocking writer lease, released by closing the file.
 * @param file Open state-root lock file.
 * @return OS locking failure when another process owns this state root.
 */
func lockStateFile(file *os.File) error {
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{})
}
