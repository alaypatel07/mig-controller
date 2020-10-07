/*
Copyright 2019 Red Hat Inc.

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

package migstorage

import (
	"context"
	"time"

	"github.com/konveyor/controller/pkg/logging"
	migapi "github.com/konveyor/mig-controller/pkg/apis/migration/v1alpha1"
	migref "github.com/konveyor/mig-controller/pkg/reference"
	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logging.WithName("storage")

// Add creates a new MigStorage Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileMigStorage{Client: mgr.GetClient(), scheme: mgr.GetScheme(), EventRecorder: mgr.GetRecorder("migstorage_controller")}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("migstorage-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to MigStorage
	err = c.Watch(
		&source.Kind{Type: &migapi.MigStorage{}},
		&handler.EnqueueRequestForObject{},
		&StoragePredicate{})
	if err != nil {
		return err
	}

	// Watch for changes to Secrets referenced by MigStorage.
	err = c.Watch(
		&source.Kind{Type: &kapi.Secret{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(
				func(a handler.MapObject) []reconcile.Request {
					return migref.GetRequests(a, migapi.MigStorage{})
				}),
		})
	if err != nil {
		return err
	}

	// Watch for changes to cloud providers.
	err = c.Watch(
		&ProviderSource{
			Client:   mgr.GetClient(),
			Interval: time.Second * 30},
		&handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileMigStorage{}

// ReconcileMigStorage reconciles a MigStorage object
type ReconcileMigStorage struct {
	client.Client
	record.EventRecorder
	scheme *runtime.Scheme
}

func (r *ReconcileMigStorage) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	var err error
	log.Reset()
	log.SetValues("storage", request)

	// Fetch the MigStorage instance
	storage := &migapi.MigStorage{}
	err = r.Get(context.TODO(), request.NamespacedName, storage)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Report reconcile error.
	defer func() {
		log.Info("CR", "conditions", storage.Status.Conditions)
		storage.Status.Conditions.RecordEvents(storage, r.EventRecorder)
		if err == nil || errors.IsConflict(err) {
			return
		}
		storage.Status.SetReconcileFailed(err)
		err := r.Update(context.TODO(), storage)
		if err != nil {
			log.Trace(err)
			return
		}
	}()

	// Begin staging conditions.
	storage.Status.BeginStagingConditions()

	// Validations.
	err = r.validate(storage)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Ready
	storage.Status.SetReady(
		!storage.Status.HasBlockerCondition(),
		"The storage is ready.")

	// End staging conditions.
	storage.Status.EndStagingConditions()

	// Apply changes.
	storage.MarkReconciled()
	err = r.Update(context.TODO(), storage)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Done
	return reconcile.Result{}, nil
}
