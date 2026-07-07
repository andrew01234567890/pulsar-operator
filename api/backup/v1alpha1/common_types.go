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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
)

// BackupDestination configures the object-store target backup artifacts are
// written to (or, for a Restore, read from). It intentionally mirrors
// cluster/v1alpha1.OffloadSpec's driver/bucket/region/endpoint/
// credentialsSecretRef shape for consistency across the operator, plus a
// prefix so backup artifacts get their own key namespace when sharing a
// bucket with tiered-storage offload data. It is a parallel definition, not
// an import of the cluster group's type: API groups must not cross-import,
// so the reconciler re-wires credentials at runtime using the same
// credentialsSecretRef convention.
// +kubebuilder:validation:XValidation:rule="self.driver == 'filesystem' || size(self.bucket) > 0",message="bucket is required for object-store destination drivers (aws-s3, google-cloud-storage, azureblob)"
type BackupDestination struct {
	// driver selects the object-storage backend.
	// +required
	// +kubebuilder:validation:Enum=aws-s3;google-cloud-storage;azureblob;filesystem
	Driver string `json:"driver"`

	// bucket is the object-storage bucket/container backup artifacts are
	// written into. Not used for the filesystem driver.
	// +optional
	Bucket string `json:"bucket,omitempty"`

	// region is the object-storage region. Not used for the filesystem driver.
	// +optional
	Region string `json:"region,omitempty"`

	// endpoint overrides the object-storage service endpoint, for
	// S3-compatible stores that aren't AWS.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// prefix is a key/path prefix under the bucket (or filesystem root) that
	// namespaces this resource's backup artifacts, so multiple Backup or
	// BackupSchedule resources can share one bucket without collision.
	// +optional
	Prefix string `json:"prefix,omitempty"`

	// credentialsSecretRef references the Secret holding the destination
	// driver's object-storage credentials. The Secret's data keys must match
	// what the selected driver expects, mirroring the tiered-storage offload
	// convention:
	//   - aws-s3: keys AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
	//   - azureblob: keys AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_ACCESS_KEY.
	//   - google-cloud-storage: a single key named "key.json" holding the
	//     service-account JSON (it is mounted as a file and pointed at by
	//     GOOGLE_APPLICATION_CREDENTIALS). A GCS credentials Secret that stores
	//     the JSON under any other data key will fail to mount and the backup
	//     Job will not start.
	//   - filesystem: not used.
	// +optional
	CredentialsSecretRef *corev1.LocalObjectReference `json:"credentialsSecretRef,omitempty"`
}
