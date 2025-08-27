package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bodgit/sevenzip"
)

func Extract7z(src, dest string) error {
	r, err := sevenzip.OpenReader(src)
	if err != nil {
		return fmt.Errorf("open 7z: %w", err)
	}
	defer r.Close()

	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("couldn't create destination folder %q: %w", dest, err)
	}

	if err := removeContents(dest); err != nil {
		return fmt.Errorf("couldn't clear destination folder %q: %w", dest, err)
	}

	destAbs, err := filepath.Abs(dest)
	if err != nil {
		return fmt.Errorf("abs dest: %w", err)
	}

	commonPrefix := detectCommonTopLevel(r.File)

	for _, f := range r.File {
		info := f.FileInfo()

		if info.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("refusing to extract symlink entry: %s", f.Name)
		}

		entryName := f.Name
		if commonPrefix != "" {
			entryName = strings.TrimPrefix(entryName, commonPrefix+"/")
		}

		target := filepath.Join(dest, filepath.FromSlash(entryName))
		if !withinBase(destAbs, target) {
			return fmt.Errorf("entry escapes destination: %s", f.Name)
		}

		if info.IsDir() {
			if err := os.MkdirAll(target, dirPerm(info.Mode())); err != nil {
				return fmt.Errorf("mkdir: %w", err)
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("mkdir parent: %w", err)
		}

		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_EXCL, info.Mode().Perm())
		if err != nil {
			return fmt.Errorf("create: %w", err)
		}

		rc, err := f.Open()
		if err != nil {
			out.Close()
			return fmt.Errorf("open entry: %w", err)
		}

		if _, err := io.Copy(out, rc); err != nil {
			out.Close()
			rc.Close()
			return fmt.Errorf("copy: %w", err)
		}

		if err := out.Close(); err != nil {
			rc.Close()
			return fmt.Errorf("close dest: %w", err)
		}
		if err := rc.Close(); err != nil {
			return fmt.Errorf("close entry: %w", err)
		}

		_ = os.Chmod(target, info.Mode().Perm())
		_ = os.Chtimes(target, time.Now(), info.ModTime())
	}

	return nil
}

func withinBase(base, path string) bool {
	abs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(base, abs)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func dirPerm(mode fs.FileMode) os.FileMode {
	p := mode.Perm()
	// Ensure execute bits so directories are traversable.
	if p&0o400 != 0 {
		p |= 0o100
	}
	if p&0o040 != 0 {
		p |= 0o010
	}
	if p&0o004 != 0 {
		p |= 0o001
	}
	return p
}

func detectCommonTopLevel(files []*sevenzip.File) string {
	var common string
	for _, f := range files {
		name := strings.TrimPrefix(f.Name, "./") // normalize
		parts := strings.SplitN(name, "/", 2)
		if len(parts) < 2 {
			// No slash in name → not in a top-level folder
			return ""
		}
		top := parts[0]
		if common == "" {
			common = top
		} else if common != top {
			return "" // Different top-level folder → no flattening
		}
	}
	return common
}

func removeContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

// Is7z checks whether the file at path appears to be a 7z archive by reading its magic number (first 6 bytes).
func Is7z(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	magic := make([]byte, 6)
	n, err := f.Read(magic)
	if err != nil {
		return false, fmt.Errorf("read magic: %w", err)
	}
	if n < 6 {
		return false, nil // too short to be a valid 7z file
	}

	return hex.EncodeToString(magic) == "377abcaf271c", nil
}
