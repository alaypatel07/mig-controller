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

package migplan

import (
	"context"
	"fmt"
	"github.com/konveyor/mig-controller/pkg/compat"
	"github.com/konveyor/mig-controller/pkg/settings"
	projectv1 "github.com/openshift/api/project/v1"
	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"strconv"

	migapi "github.com/konveyor/mig-controller/pkg/apis/migration/v1alpha1"
	migctl "github.com/konveyor/mig-controller/pkg/controller/migmigration"
	"github.com/konveyor/mig-controller/pkg/logging"
	migref "github.com/konveyor/mig-controller/pkg/reference"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logging.WithName("plan")

// Application settings.
var Settings = &settings.Settings

// Add creates a new MigPlan Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileMigPlan{Client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("migplan-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		log.Trace(err)
		return err
	}

	// Watch for changes to MigPlan
	err = c.Watch(&source.Kind{
		Type: &migapi.MigPlan{}},
		&handler.EnqueueRequestForObject{},
		&PlanPredicate{},
	)
	if err != nil {
		log.Trace(err)
		return err
	}

	// Watch for changes to MigClusters referenced by MigPlans
	err = c.Watch(
		&source.Kind{Type: &migapi.MigCluster{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(
				func(a handler.MapObject) []reconcile.Request {
					return migref.GetRequests(a, migapi.MigPlan{})
				}),
		},
		&ClusterPredicate{})
	if err != nil {
		log.Trace(err)
		return err
	}

	// Watch for changes to MigStorage referenced by MigPlans
	err = c.Watch(
		&source.Kind{Type: &migapi.MigStorage{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(
				func(a handler.MapObject) []reconcile.Request {
					return migref.GetRequests(a, migapi.MigPlan{})
				}),
		},
		&StoragePredicate{})
	if err != nil {
		log.Trace(err)
		return err
	}

	// Watch for changes to MigMigrations.
	err = c.Watch(
		&source.Kind{Type: &migapi.MigMigration{}},
		&handler.EnqueueRequestsFromMapFunc{
			ToRequests: handler.ToRequestsFunc(MigrationRequests),
		},
		&MigrationPredicate{})
	if err != nil {
		log.Trace(err)
		return err
	}

	// Indexes
	indexer := mgr.GetFieldIndexer()

	// Plan
	err = indexer.IndexField(
		&migapi.MigPlan{},
		migapi.ClosedIndexField,
		func(rawObj runtime.Object) []string {
			p, cast := rawObj.(*migapi.MigPlan)
			if !cast {
				return nil
			}
			return []string{
				strconv.FormatBool(p.Spec.Closed),
			}
		})
	if err != nil {
		log.Trace(err)
		return err
	}
	// Pod
	err = indexer.IndexField(
		&kapi.Pod{},
		"status.phase",
		func(rawObj runtime.Object) []string {
			p, cast := rawObj.(*kapi.Pod)
			if !cast {
				return nil
			}
			return []string{
				string(p.Status.Phase),
			}
		})
	if err != nil {
		log.Trace(err)
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileMigPlan{}

// ReconcileMigPlan reconciles a MigPlan object
type ReconcileMigPlan struct {
	client.Client
	scheme *runtime.Scheme
}

func (r *ReconcileMigPlan) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	var err error
	log.Reset()

	// Fetch the MigPlan instance
	plan := &migapi.MigPlan{}
	err = r.Get(context.TODO(), request.NamespacedName, plan)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		log.Trace(err)
		return reconcile.Result{}, err
	}

	// Report reconcile error.
	defer func() {
		if err == nil || errors.IsConflict(err) {
			return
		}
		plan.Status.SetReconcileFailed(err)
		err := r.Update(context.TODO(), plan)
		if err != nil {
			log.Trace(err)
			return
		}
	}()

	// Plan closed.
	closed, err := r.handleClosed(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}
	if closed {
		return reconcile.Result{}, nil
	}

	// Begin staging conditions.
	plan.Status.BeginStagingConditions()

	// Plan Suspended
	err = r.planSuspended(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Validations.
	err = r.validate(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	err = r.deployVelero(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	err = r.waitForMigControllerReady(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// PV discovery
	err = r.updatePvs(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Validate NFS PV accessibility.
	nfsValidation := NfsValidation{Plan: plan}
	err = nfsValidation.Run(r.Client)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Validate PV actions.
	err = r.validatePvSelections(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Storage
	err = r.ensureStorage(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Migration Registry
	err = r.ensureMigRegistries(plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Ready
	plan.Status.SetReady(
		plan.Status.HasCondition(StorageEnsured, PvsDiscovered, RegistriesEnsured) &&
			!plan.Status.HasBlockerCondition(),
		ReadyMessage)

	// End staging conditions.
	plan.Status.EndStagingConditions()

	// Apply changes.
	plan.MarkReconciled()
	err = r.Update(context.TODO(), plan)
	if err != nil {
		log.Trace(err)
		return reconcile.Result{Requeue: true}, nil
	}

	// Done
	return reconcile.Result{}, nil
}

// Detect that a plan is been closed and ensure all its referenced
// resources have been cleaned up.
func (r *ReconcileMigPlan) handleClosed(plan *migapi.MigPlan) (bool, error) {
	closed := plan.Spec.Closed
	if !closed || plan.Status.HasCondition(Closed) {
		return closed, nil
	}

	plan.MarkReconciled()
	plan.Status.SetReady(false, ReadyMessage)
	err := r.Update(context.TODO(), plan)
	if err != nil {
		return closed, err
	}

	err = r.ensureClosed(plan)
	return closed, err
}

// Ensure that resources managed by the plan have been cleaned up.
func (r *ReconcileMigPlan) ensureClosed(plan *migapi.MigPlan) error {
	clusters, err := migapi.ListClusters(r)
	if err != nil {
		log.Trace(err)
		return err
	}
	for _, cluster := range clusters {
		if !cluster.Status.IsReady() {
			continue
		}
		err = cluster.DeleteResources(r, plan.GetCorrelationLabels())
		if err != nil {
			log.Trace(err)
			return err
		}
	}
	plan.Status.DeleteCondition(StorageEnsured, RegistriesEnsured, Suspended)
	plan.Status.SetCondition(migapi.Condition{
		Type:     Closed,
		Status:   True,
		Category: Advisory,
		Message:  ClosedMessage,
	})
	// Apply changes.
	plan.MarkReconciled()
	err = r.Update(context.TODO(), plan)
	if err != nil {
		log.Trace(err)
		return err
	}

	return nil
}

// Determine whether the plan is `suspended`.
// A plan is considered`suspended` when a migration is running or the final migration has
// completed successfully. While suspended, reconcile is limited to basic validation
// and PV discovery and ensuring resources is not performed.
func (r *ReconcileMigPlan) planSuspended(plan *migapi.MigPlan) error {
	suspended := false
	migrations, err := plan.ListMigrations(r)
	if err != nil {
		log.Trace(err)
		return err
	}
	for _, m := range migrations {
		if m.Status.HasCondition(migctl.Running) {
			suspended = true
			break
		}
		if m.Status.HasCondition(migctl.Succeeded) && !m.Spec.Stage {
			suspended = true
			break
		}
	}

	if suspended {
		plan.Status.SetCondition(migapi.Condition{
			Type:     Suspended,
			Status:   True,
			Category: Advisory,
			Message:  SuspendedMessage,
		})
	}

	return nil
}

func (r ReconcileMigPlan) deployVelero(plan *migapi.MigPlan) error {
	clients, err := r.getBothClients(plan)
	if err != nil {
		return err
	}
	resources := r.getDeployVeleroResources(plan)

	for _, client := range clients {
		for _, r := range resources {
			u := r.DeepCopy()
			err = client.Create(context.Background(), u)
			switch {
			case errors.IsAlreadyExists(err):
			case err != nil:
				return err
			}
			log.Info("created resource",
				u.GroupVersionKind().String(),
				u.GetNamespace()+"/"+u.GetName(),
			)
		}
	}

	return nil
}

func (r ReconcileMigPlan) getDeployVeleroResources(plan *migapi.MigPlan) []*unstructured.Unstructured {
	projectRequestUnstructured := &unstructured.Unstructured{}
	projectRequestUnstructured.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   projectv1.GroupName,
		Version: projectv1.GroupVersion.Version,
		Kind:    "ProjectRequest",
	})
	projectRequestUnstructured.SetName(plan.Name)
	projectRequestUnstructured.SetLabels(map[string]string{"migration.openshift.io/migplan": plan.GetName()})
	migrationController := &unstructured.Unstructured{}
	migrationController.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "migration.openshift.io",
		Version: "v1alpha1",
		Kind:    "MigrationController",
	})
	migrationController.SetLabels(map[string]string{"migration.openshift.io/migplan": plan.GetName()})
	migrationController.SetName("migration-controller")
	migrationController.SetNamespace(plan.Name)
	err := unstructured.SetNestedMap(migrationController.Object, map[string]interface{}{
		"azure_resource_group": "",
		"cluster_name":         "host",
		"migration_velero":     "true",
		"migration_controller": "false",
		"migration_ui":         "false",
		"restic_timeout":       "1h",
	}, "spec")
	if err != nil {
		// TODO: handle this error better
		panic(err)
	}
	return []*unstructured.Unstructured{projectRequestUnstructured, migrationController}
}

func (r ReconcileMigPlan) waitForMigControllerReady(plan *migapi.MigPlan) error {
	migrationController := &unstructured.Unstructured{}
	migrationController.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "migration.openshift.io",
		Version: "v1alpha1",
		Kind:    "MigrationController",
	})
	migrationController.SetLabels(map[string]string{"migration.openshift.io/migplan": plan.GetName()})
	migrationController.SetName("migration-controller")
	migrationController.SetNamespace(plan.Name)

	clients, err := r.getBothClients(plan)
	if err != nil {
		return err
	}
	for _, c := range clients {
		u := &unstructured.Unstructured{}
		err := c.Get(context.Background(), types.NamespacedName{
			Namespace: migrationController.GetName(),
			Name:      migrationController.GetNamespace(),
		}, u)
		if err != nil {
			return nil
		}
		status, found, err := unstructured.NestedString(u.Object, "status", "phase")
		switch {
		case err != nil:
			return err
		case !found:
			return fmt.Errorf("waiting for status in plan's migration controller")
		case status != "Reconciled":
			return fmt.Errorf("waiting for status.phase == Reconciled in plan's migration controller")
		}
	}
	return nil
}

func (r ReconcileMigPlan) getBothClients(plan *migapi.MigPlan) ([]compat.Client, error) {
	sourceRef := plan.Spec.SrcMigClusterRef

	// NotSet
	if !migref.RefSet(sourceRef) {
		plan.Status.SetCondition(migapi.Condition{
			Type:     InvalidSourceClusterRef,
			Status:   True,
			Reason:   NotSet,
			Category: Critical,
			Message:  InvalidSourceClusterRefMessage,
		})
		return nil, nil
	}

	sourceCluster, err := migapi.GetCluster(r, sourceRef)
	if err != nil {
		log.Trace(err)
		return nil, err
	}

	sourceClient, err := sourceCluster.GetClient(r.Client)
	if err != nil {
		log.Trace(err)
		return nil, err
	}

	destRef := plan.Spec.DestMigClusterRef

	// NotSet
	if !migref.RefSet(destRef) {
		plan.Status.SetCondition(migapi.Condition{
			Type:     InvalidDestinationClusterRef,
			Status:   True,
			Reason:   NotSet,
			Category: Critical,
			Message:  InvalidSourceClusterRefMessage,
		})
		return nil, nil
	}

	destinationCluster, err := migapi.GetCluster(r, destRef)
	if err != nil {
		log.Trace(err)
		return nil, err
	}

	destinationClient, err := destinationCluster.GetClient(r.Client)
	if err != nil {
		log.Trace(err)
		return nil, err
	}
	return []compat.Client{sourceClient, destinationClient}, nil
}
