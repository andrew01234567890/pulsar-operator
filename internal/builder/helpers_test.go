package builder

// Shared literals for table-driven tests across this package, so repeated
// values (a goconst violation) live in one place instead of being retyped
// per test file.
const (
	testClusterName  = "pulsar-cluster"
	testClusterName2 = "my-pulsar"
	testNamespace    = "pulsar-ns"

	testComponentBroker          = "broker"
	testComponentBookkeeper      = "bookkeeper"
	testComponentFunctionsWorker = "functions-worker"

	testNameBroker     = "pulsar-broker"
	testNameBookkeeper = "pulsar-bookkeeper"
)
