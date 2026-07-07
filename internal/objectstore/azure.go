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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// Azure shared-key credential env vars, wired from the destination's
// credentialsSecretRef exactly as the offload feature wires them onto the
// broker (see internal/controller/cluster/pulsarcluster_offload.go).
const (
	envAzureStorageAccount   = "AZURE_STORAGE_ACCOUNT"
	envAzureStorageAccessKey = "AZURE_STORAGE_ACCESS_KEY"
)

// azureStore uploads/downloads blobs to an Azure Blob Storage container
// (Config.Bucket). It authenticates with a shared-key credential built from
// the AZURE_STORAGE_ACCOUNT / AZURE_STORAGE_ACCESS_KEY env vars.
type azureStore struct {
	cfg    Config
	client *azblob.Client
}

func newAzureStore(cfg Config) (*azureStore, error) {
	// An inline credential (see Config's doc) overrides the environment - used
	// by the Restore reconciler's in-process manifest-header peek, which
	// resolves credentialsSecretRef itself rather than relying on env vars the
	// way the export/import Job does.
	account := cfg.AzureAccount
	key := cfg.AzureAccessKey
	if account == "" {
		account = os.Getenv(envAzureStorageAccount)
	}
	if key == "" {
		key = os.Getenv(envAzureStorageAccessKey)
	}
	if account == "" || key == "" {
		return nil, fmt.Errorf("objectstore: azureblob requires %s and %s in the environment",
			envAzureStorageAccount, envAzureStorageAccessKey)
	}

	cred, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		return nil, fmt.Errorf("objectstore: azure shared-key credential: %w", err)
	}

	serviceURL := cfg.Endpoint
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net/", account)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("objectstore: create azure client: %w", err)
	}
	return &azureStore{cfg: cfg, client: client}, nil
}

func (s *azureStore) Upload(ctx context.Context, key string, r io.Reader) error {
	full := resolveKey(s.cfg, key)
	if _, err := s.client.UploadStream(ctx, s.cfg.Bucket, full, r, nil); err != nil {
		return fmt.Errorf("objectstore: upload azblob://%s/%s: %w", s.cfg.Bucket, full, err)
	}
	return nil
}

func (s *azureStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	full := resolveKey(s.cfg, key)
	resp, err := s.client.DownloadStream(ctx, s.cfg.Bucket, full, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, &notExistError{err: fmt.Errorf("objectstore: download azblob://%s/%s: %w", s.cfg.Bucket, full, err)}
		}
		return nil, fmt.Errorf("objectstore: download azblob://%s/%s: %w", s.cfg.Bucket, full, err)
	}
	return resp.Body, nil
}

func (s *azureStore) URI(key string) string {
	return URI(s.cfg, key)
}
