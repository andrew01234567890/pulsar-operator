/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package objectstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// filesystemStore writes blobs to a mounted volume path. Bucket is the base
// directory (defaulting to DefaultFilesystemRoot); Prefix and key nest beneath
// it. It is the one driver exercised end-to-end without a cloud account, so
// it doubles as the local round-trip test backend for this package.
type filesystemStore struct {
	cfg Config
}

func newFilesystemStore(cfg Config) *filesystemStore {
	return &filesystemStore{cfg: cfg}
}

// filesystemPath resolves cfg+key to an absolute on-disk path:
// <bucket-or-default>/<prefix>/<key>.
func filesystemPath(cfg Config, key string) string {
	root := cfg.Bucket
	if root == "" {
		root = DefaultFilesystemRoot
	}
	return filepath.Join(root, cfg.Prefix, key)
}

func (s *filesystemStore) Upload(_ context.Context, key string, r io.Reader) error {
	dst := filesystemPath(s.cfg, key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		return fmt.Errorf("objectstore: create directory for %q: %w", dst, err)
	}
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("objectstore: create %q: %w", dst, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return fmt.Errorf("objectstore: write %q: %w", dst, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("objectstore: close %q: %w", dst, err)
	}
	return nil
}

func (s *filesystemStore) Download(_ context.Context, key string) (io.ReadCloser, error) {
	src := filesystemPath(s.cfg, key)
	f, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &notExistError{err: fmt.Errorf("objectstore: open %q: %w", src, err)}
		}
		return nil, fmt.Errorf("objectstore: open %q: %w", src, err)
	}
	return f, nil
}

func (s *filesystemStore) URI(key string) string {
	return URI(s.cfg, key)
}
