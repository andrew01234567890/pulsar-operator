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

// Package objectstore is the operator's driver-agnostic blob upload/download
// abstraction over the four object-storage backends a backup.pulsaroperator.io
// BackupDestination can name (aws-s3, google-cloud-storage, azureblob,
// filesystem). It is shared by the Backup reconciler's export Job (upload) and
// the Restore reconciler's import Job (download) so both speak the identical
// interface regardless of driver.
//
// Credentials are NOT carried in Config: they are read from the process
// environment exactly as the tiered-storage offload feature wires them onto a
// pod (see internal/controller/cluster/pulsarcluster_offload.go), so the
// operator mounts a BackupDestination.credentialsSecretRef onto the Job the
// same way and each driver's SDK picks the credentials up from its standard
// source:
//
//   - aws-s3: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env (the AWS SDK's
//     default credential chain).
//   - azureblob: AZURE_STORAGE_ACCOUNT / AZURE_STORAGE_ACCESS_KEY env (a
//     shared-key credential).
//   - google-cloud-storage: GOOGLE_APPLICATION_CREDENTIALS, which is a PATH to
//     the service-account JSON key file the operator mounts as a volume - never
//     the key content itself.
//   - filesystem: no credentials; a mounted volume path.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
)

// Driver identifiers, mirroring backup/v1alpha1.BackupDestination.Driver's
// kubebuilder enum exactly.
const (
	DriverAWSS3      = "aws-s3"
	DriverGCS        = "google-cloud-storage"
	DriverAzureBlob  = "azureblob"
	DriverFilesystem = "filesystem"
)

// Config identifies WHERE a Store reads and writes, but never HOW it
// authenticates (see the package doc): it mirrors the
// driver/bucket/region/endpoint/prefix shape of BackupDestination so a
// reconciler can hand its spec.destination straight through.
type Config struct {
	// Driver selects the backend; one of the Driver* constants.
	Driver string

	// Bucket is the S3 bucket / GCS bucket / Azure container name. For the
	// filesystem driver it is the base directory every key is written under
	// (a mounted volume path), defaulting to DefaultFilesystemRoot when empty.
	Bucket string

	// Region is the object-storage region (aws-s3, google-cloud-storage).
	// Ignored by azureblob and filesystem.
	Region string

	// Endpoint overrides the service endpoint for S3-compatible stores that
	// aren't AWS (e.g. MinIO), or the Azure blob service URL. Ignored by the
	// filesystem driver.
	Endpoint string

	// Prefix is a key/path prefix applied under Bucket to every key, so
	// several backups can share one bucket without collision. It is joined
	// with the per-call key by Store implementations and by URI.
	Prefix string

	// The fields below are an EXCEPTION to the package doc's "credentials are
	// never carried in Config" rule, added specifically for the Restore
	// reconciler's cookie-lineage pre-flight check: unlike the export/import
	// Job (which always runs in its own pod and picks up credentials from its
	// own environment/mounted volume), the reconciler needs to peek at a
	// manifest's header from inside the shared manager process, where it
	// cannot safely mutate the process environment on every Restore's behalf.
	// Instead it resolves the destination's credentialsSecretRef itself and
	// passes the values through here. Left empty (the Job's own path always
	// leaves them empty), every driver falls back to its normal
	// environment/default-credential-chain behavior, unchanged.

	// AWSAccessKeyID / AWSSecretAccessKey optionally provide a static AWS
	// credential inline, bypassing the AWS SDK's default chain (env vars,
	// shared config, IRSA/instance profile).
	AWSAccessKeyID     string
	AWSSecretAccessKey string

	// AzureAccount / AzureAccessKey optionally provide an Azure shared-key
	// credential inline instead of via the AZURE_STORAGE_ACCOUNT /
	// AZURE_STORAGE_ACCESS_KEY environment variables.
	AzureAccount   string
	AzureAccessKey string

	// GCSCredentialsJSON optionally provides a GCS service-account key inline
	// instead of via GOOGLE_APPLICATION_CREDENTIALS (a mounted file path). A
	// string (not []byte) so Config stays comparable with ==/!=, which
	// existing tests (and BackupDestination-derived Config values in
	// general) rely on.
	GCSCredentialsJSON string
}

// DefaultFilesystemRoot is the base directory the filesystem driver writes
// under when Config.Bucket is empty. It is the conventional mount path the
// operator projects a backup volume at.
const DefaultFilesystemRoot = "/backups"

// Store is the driver-agnostic blob interface the Backup export Job and the
// Restore import Job both consume. Upload and Download address a blob by a
// caller-supplied key, relative to Config.Prefix (implementations join the
// two); URI reports the fully-qualified location the same join resolves to,
// for surfacing in a Backup's status.artifactURI without needing credentials
// or a network round-trip.
type Store interface {
	// Upload writes r's full contents to the blob at key (under Config.Prefix),
	// overwriting any existing blob. It reads r to EOF.
	Upload(ctx context.Context, key string, r io.Reader) error

	// Download opens the blob at key (under Config.Prefix) for reading. The
	// caller must Close the returned ReadCloser. A missing blob is reported as
	// an error for which IsNotExist returns true.
	Download(ctx context.Context, key string) (io.ReadCloser, error)

	// URI returns the fully-qualified, driver-scheme location of key (under
	// Config.Prefix): s3://, gs://, azblob://, or file://. It performs no I/O.
	URI(key string) string
}

// New constructs the Store for cfg.Driver, wiring the driver's SDK to read
// credentials from the process environment (see the package doc). It fails
// fast on an unknown driver so a misconfigured BackupDestination surfaces as a
// clear reconciler error rather than a nil Store.
func New(ctx context.Context, cfg Config) (Store, error) {
	switch cfg.Driver {
	case DriverFilesystem:
		return newFilesystemStore(cfg), nil
	case DriverAWSS3:
		return newS3Store(ctx, cfg)
	case DriverGCS:
		return newGCSStore(ctx, cfg)
	case DriverAzureBlob:
		return newAzureStore(cfg)
	default:
		return nil, fmt.Errorf("objectstore: unsupported driver %q", cfg.Driver)
	}
}

// URI reports the fully-qualified location cfg+key resolves to, WITHOUT
// constructing a Store or contacting the backend - a pure string function a
// reconciler can call to fill status.artifactURI even when it holds no
// object-store credentials of its own (those live only on the Job pod). It
// matches the URI a Store built from the same cfg returns for the same key.
func URI(cfg Config, key string) string {
	full := resolveKey(cfg, key)
	switch cfg.Driver {
	case DriverAWSS3:
		return "s3://" + path.Join(cfg.Bucket, full)
	case DriverGCS:
		return "gs://" + path.Join(cfg.Bucket, full)
	case DriverAzureBlob:
		return "azblob://" + path.Join(cfg.Bucket, full)
	case DriverFilesystem:
		return "file://" + filesystemPath(cfg, key)
	default:
		return ""
	}
}

// resolveKey joins Config.Prefix with a per-call key into the object key used
// within a bucket/container. It is not used for the filesystem driver, which
// resolves to an on-disk path via filesystemPath.
func resolveKey(cfg Config, key string) string {
	if cfg.Prefix == "" {
		return key
	}
	return path.Join(cfg.Prefix, key)
}

// KeyFromURI inverts URI: given the same cfg that produced uri, it recovers
// the raw per-call key (relative to Config.Prefix) that a Store's
// Upload/Download/URI expect. This is the Restore reconciler's counterpart to
// a Backup's status.artifactURI - a Restore only carries the fully-qualified
// URI (spec.source.artifactURI), but Store.Download needs a bare key, so this
// recovers it without any I/O.
func KeyFromURI(cfg Config, uri string) (string, error) {
	scheme, ok := uriScheme(cfg.Driver)
	if !ok {
		return "", fmt.Errorf("objectstore: unsupported driver %q", cfg.Driver)
	}
	rest, ok := strings.CutPrefix(uri, scheme)
	if !ok {
		return "", fmt.Errorf("objectstore: artifact URI %q does not have the %s scheme expected for driver %q", uri, scheme, cfg.Driver)
	}

	if cfg.Driver == DriverFilesystem {
		root := cfg.Bucket
		if root == "" {
			root = DefaultFilesystemRoot
		}
		base := filepath.Join(root, cfg.Prefix)
		rel, err := filepath.Rel(base, rest)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("objectstore: artifact URI %q is not under %q", uri, base)
		}
		return rel, nil
	}

	bucketPrefix := cfg.Bucket + "/"
	full, ok := strings.CutPrefix(rest, bucketPrefix)
	if !ok {
		return "", fmt.Errorf("objectstore: artifact URI %q does not belong to bucket %q", uri, cfg.Bucket)
	}
	if cfg.Prefix != "" {
		keyPrefix := cfg.Prefix + "/"
		key, ok := strings.CutPrefix(full, keyPrefix)
		if !ok {
			return "", fmt.Errorf("objectstore: artifact URI %q does not have prefix %q", uri, cfg.Prefix)
		}
		full = key
	}
	return full, nil
}

// uriScheme returns the URI scheme (including "://") a driver's URI uses.
func uriScheme(driver string) (string, bool) {
	switch driver {
	case DriverAWSS3:
		return "s3://", true
	case DriverGCS:
		return "gs://", true
	case DriverAzureBlob:
		return "azblob://", true
	case DriverFilesystem:
		return "file://", true
	default:
		return "", false
	}
}

// IsNotExist reports whether err indicates the requested blob does not exist,
// normalizing each driver's distinct not-found error so callers (notably the
// Restore reconciler) can branch on a missing artifact uniformly.
func IsNotExist(err error) bool {
	var nfe *notExistError
	return errors.As(err, &nfe)
}

// notExistError wraps a driver's not-found error in a uniform, IsNotExist
// -recognizable type without discarding the original error's message.
type notExistError struct{ err error }

func (e *notExistError) Error() string { return e.err.Error() }
func (e *notExistError) Unwrap() error { return e.err }
