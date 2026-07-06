package builder

import (
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// SetControllerOwner sets owner as controlled's controller owner
// reference, so the garbage collector deletes controlled when owner is
// deleted and a Watch+EnqueueRequestForOwner requeues owner when controlled
// changes.
func SetControllerOwner(owner, controlled client.Object, scheme *runtime.Scheme) error {
	return controllerutil.SetControllerReference(owner, controlled, scheme)
}
