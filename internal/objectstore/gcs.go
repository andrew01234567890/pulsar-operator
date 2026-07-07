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
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// gcsStore uploads/downloads blobs to a Google Cloud Storage bucket.
// Credentials come from GOOGLE_APPLICATION_CREDENTIALS, which the storage
// client reads as a PATH to a service-account JSON key file - the operator
// mounts the destination's credentialsSecretRef as a volume and points that
// env var at the mount, never at the key content (mirrors the offload feature).
type gcsStore struct {
	cfg    Config
	client *storage.Client
}

func newGCSStore(ctx context.Context, cfg Config) (*gcsStore, error) {
	var opts []option.ClientOption
	if cfg.GCSCredentialsJSON != "" {
		// An inline credential (see Config's doc) overrides the default
		// Application Default Credentials chain - used by the Restore
		// reconciler's in-process manifest-header peek, which resolves
		// credentialsSecretRef itself rather than mounting a key file the
		// way the export/import Job does. WithAuthCredentialsJSON (rather
		// than the deprecated WithCredentialsJSON) pins the expected
		// credential type: the Secret this comes from is documented (see
		// BackupDestination) to hold a service-account key, exactly like the
		// Job's own GOOGLE_APPLICATION_CREDENTIALS file mount expects.
		opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(cfg.GCSCredentialsJSON)))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("objectstore: create GCS client: %w", err)
	}
	return &gcsStore{cfg: cfg, client: client}, nil
}

func (s *gcsStore) Upload(ctx context.Context, key string, r io.Reader) error {
	full := resolveKey(s.cfg, key)
	w := s.client.Bucket(s.cfg.Bucket).Object(full).NewWriter(ctx)
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return fmt.Errorf("objectstore: write gs://%s/%s: %w", s.cfg.Bucket, full, err)
	}
	// The write is only committed on Close; a failure here (not on Copy) is
	// where GCS surfaces a rejected object, so it must be reported.
	if err := w.Close(); err != nil {
		return fmt.Errorf("objectstore: commit gs://%s/%s: %w", s.cfg.Bucket, full, err)
	}
	return nil
}

func (s *gcsStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	full := resolveKey(s.cfg, key)
	r, err := s.client.Bucket(s.cfg.Bucket).Object(full).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, &notExistError{err: fmt.Errorf("objectstore: read gs://%s/%s: %w", s.cfg.Bucket, full, err)}
		}
		return nil, fmt.Errorf("objectstore: read gs://%s/%s: %w", s.cfg.Bucket, full, err)
	}
	return r, nil
}

func (s *gcsStore) URI(key string) string {
	return URI(s.cfg, key)
}
