package metadata

import "testing"

const testServiceName = "myoxia-oxia"

func TestPublicServiceName(t *testing.T) {
	tests := []struct {
		name            string
		oxiaClusterName string
		want            string
	}{
		{name: "simple name", oxiaClusterName: "myoxia", want: testServiceName},
		{name: "hyphenated name", oxiaClusterName: "pulsar-cluster-oxia", want: "pulsar-cluster-oxia-oxia"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := PublicServiceName(tt.oxiaClusterName); got != tt.want {
				t.Errorf("PublicServiceName(%q) = %q, want %q", tt.oxiaClusterName, got, tt.want)
			}
		})
	}
}

func TestMetadataStoreURL(t *testing.T) {
	tests := []struct {
		name          string
		serviceName   string
		oxiaNamespace string
		want          string
	}{
		{
			name:          "default namespace",
			serviceName:   testServiceName,
			oxiaNamespace: "default",
			want:          "oxia://myoxia-oxia:6648/default",
		},
		{
			name:          "broker namespace",
			serviceName:   testServiceName,
			oxiaNamespace: "broker",
			want:          "oxia://myoxia-oxia:6648/broker",
		},
		{
			name:          "namespace-qualified service name",
			serviceName:   "myoxia-oxia.pulsar-ns.svc.cluster.local",
			oxiaNamespace: "default",
			want:          "oxia://myoxia-oxia.pulsar-ns.svc.cluster.local:6648/default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MetadataStoreURL(tt.serviceName, tt.oxiaNamespace); got != tt.want {
				t.Errorf("MetadataStoreURL(%q, %q) = %q, want %q", tt.serviceName, tt.oxiaNamespace, got, tt.want)
			}
		})
	}
}

func TestBookkeeperMetadataServiceURI(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
		want        string
	}{
		{
			name:        "simple service name",
			serviceName: testServiceName,
			want:        "metadata-store:oxia://myoxia-oxia:6648/bookkeeper",
		},
		{
			name:        "namespace-qualified service name",
			serviceName: "myoxia-oxia.pulsar-ns.svc.cluster.local",
			want:        "metadata-store:oxia://myoxia-oxia.pulsar-ns.svc.cluster.local:6648/bookkeeper",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BookkeeperMetadataServiceURI(tt.serviceName); got != tt.want {
				t.Errorf("BookkeeperMetadataServiceURI(%q) = %q, want %q", tt.serviceName, got, tt.want)
			}
		})
	}
}

func TestMetadataStoreURLUsesPublicServiceName(t *testing.T) {
	// Regression: the coordinator's own Service must never be the target of
	// MetadataStoreURL — the coordinator only assigns shards, it does not
	// serve client reads/writes. Guard against a future refactor pointing
	// broker/bookkeeper wiring at the wrong Service.
	svc := PublicServiceName("myoxia")
	want := "oxia://myoxia-oxia:6648/default"
	if got := MetadataStoreURL(svc, "default"); got != want {
		t.Errorf("MetadataStoreURL(PublicServiceName(%q), \"default\") = %q, want %q", "myoxia", got, want)
	}
}
