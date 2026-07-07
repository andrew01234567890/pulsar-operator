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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3Store uploads/downloads blobs to an S3 (or S3-compatible, via
// Config.Endpoint) bucket. Credentials come from the AWS SDK's default chain,
// i.e. the AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars the operator
// wires from the destination's credentialsSecretRef.
type s3Store struct {
	cfg    Config
	client *s3.Client
}

func newS3Store(ctx context.Context, cfg Config) (*s3Store, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("objectstore: load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			// A custom endpoint means an S3-compatible store (e.g. MinIO),
			// which almost always needs path-style addressing since
			// <bucket>.<endpoint> virtual-host DNS rarely resolves there.
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true

			// aws-sdk-go-v2 defaults to WhenSupported, which stamps a
			// CRC32 checksum (x-amz-checksum-crc32 / x-amz-sdk-checksum-algorithm)
			// on every request; many S3-compatible stores (older MinIO, Ceph
			// RGW) reject those trailers with an XML/SignatureDoesNotMatch
			// error. A custom endpoint implies such a store, so downgrade to
			// WhenRequired - AWS proper never sets an endpoint, so this never
			// weakens real-S3 uploads.
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	})
	return &s3Store{cfg: cfg, client: client}, nil
}

func (s *s3Store) Upload(ctx context.Context, key string, r io.Reader) error {
	// PutObject signs its body, which needs a seekable reader; the manifests
	// this carries are Oxia metadata (never bulk ledger bytes), so buffering
	// in memory is an acceptable trade for not pulling in the multipart
	// upload manager.
	buf, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("objectstore: read upload body: %w", err)
	}
	full := resolveKey(s.cfg, key)
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(full),
		Body:   bytes.NewReader(buf),
	})
	if err != nil {
		return fmt.Errorf("objectstore: put s3://%s/%s: %w", s.cfg.Bucket, full, err)
	}
	return nil
}

func (s *s3Store) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	full := resolveKey(s.cfg, key)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(full),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, &notExistError{err: fmt.Errorf("objectstore: get s3://%s/%s: %w", s.cfg.Bucket, full, err)}
		}
		return nil, fmt.Errorf("objectstore: get s3://%s/%s: %w", s.cfg.Bucket, full, err)
	}
	return out.Body, nil
}

func (s *s3Store) URI(key string) string {
	return URI(s.cfg, key)
}
