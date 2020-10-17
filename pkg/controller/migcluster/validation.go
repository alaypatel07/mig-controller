package migcluster

import (
	"context"
	"fmt"
	"net/url"
	"time"

	auth "k8s.io/api/authorization/v1"

	liberr "github.com/konveyor/controller/pkg/error"
	migapi "github.com/konveyor/mig-controller/pkg/apis/migration/v1alpha1"
	migref "github.com/konveyor/mig-controller/pkg/reference"
)

// Types
const (
	InvalidURL           = "InvalidURL"
	InvalidSaSecretRef   = "InvalidSaSecretRef"
	InvalidSaToken       = "InvalidSaToken"
	TestConnectFailed    = "TestConnectFailed"
	SaTokenNotPrivileged = "SaTokenNotPrivileged"
)

// Categories
const (
	Critical = migapi.Critical
)

// Reasons
const (
	NotSet        = "NotSet"
	NotFound      = "NotFound"
	ConnectFailed = "ConnectFailed"
	Malformed     = "Malformed"
	InvalidScheme = "InvalidScheme"
	Unauthorized  = "Unauthorized"
)

// Statuses
const (
	True  = migapi.True
	False = migapi.False
)

// Messages
const (
	ReadyMessage                = "The cluster is ready."
	MissingURLMessage           = "The `url` is required when `isHostCluster` is false."
	InvalidSaSecretRefMessage   = "The `serviceAccountSecretRef` must reference a `secret`."
	InvalidSaTokenMessage       = "The `saToken` not found in `serviceAccountSecretRef` secret."
	TestConnectFailedMessage    = "Test connect failed: %s"
	MalformedURLMessage         = "The `url` is malformed."
	InvalidURLSchemeMessage     = "The `url` scheme must be 'http' or 'https'."
	SaTokenNotPrivilegedMessage = "The `saToken` has insufficient privileges."
)

// Validate the asset collection resource.
// Returns error and the total error conditions set.
func (r ReconcileMigCluster) validate(cluster *migapi.MigCluster) error {
	// General settings
	err := r.validateURL(cluster)
	if err != nil {
		return liberr.Wrap(err)
	}

	// SA secret
	err = r.validateSaSecret(cluster)
	if err != nil {
		return liberr.Wrap(err)
	}

	// Test Connection
	err = r.testConnection(cluster)
	if err != nil {
		return liberr.Wrap(err)
	}

	err = r.validateSaTokenPrivileges(cluster)
	if err != nil {
		return liberr.Wrap(err)
	}

	return nil
}

func (r ReconcileMigCluster) validateURL(cluster *migapi.MigCluster) error {
	// Not needed.
	if cluster.Spec.IsHostCluster {
		log.V(4).Info("skipping url validation as this is a host cluster",
			"name", cluster.Name, "namespace", cluster.Namespace)
		return nil
	}

	if cluster.Spec.URL == "" {
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidURL,
			Status:   True,
			Reason:   NotSet,
			Category: Critical,
			Message:  "`url` is missing for the non-host cluster",
		}, cluster.Namespace, cluster.Name)
		return nil
	}
	u, err := url.Parse(cluster.Spec.URL)
	if err != nil {
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidURL,
			Status:   True,
			Reason:   Malformed,
			Category: Critical,
			Message:  "`url` is malformed for the non-host cluster",
		}, cluster.Namespace, cluster.Name)
		return nil
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidURL,
			Status:   True,
			Reason:   InvalidScheme,
			Category: Critical,
			Message:  "`url` scheme is invalid, must be 'http' or 'https'",
		}, cluster.Namespace, cluster.Name)
		return nil
	}
	return nil
}

func (r ReconcileMigCluster) validateSaSecret(cluster *migapi.MigCluster) error {
	ref := cluster.Spec.ServiceAccountSecretRef

	// Not needed.
	if cluster.Spec.IsHostCluster {
		log.V(4).Info("skipping sa secret validation as this is a host cluster",
			"name", cluster.Name, "namespace", cluster.Namespace)
		return nil
	}

	// NotSet
	if !migref.RefSet(ref) {
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidSaSecretRef,
			Status:   True,
			Reason:   NotSet,
			Category: Critical,
			Message:  "serviceAccountSecretRef not set as a secret for the migcluster",
		}, cluster.Namespace, cluster.Name)
		return nil
	}

	secret, err := migapi.GetSecret(r, ref)
	if err != nil {
		return liberr.Wrap(fmt.Errorf("error getting migcluster sa secret %s/%s: %v",
			cluster.Spec.ServiceAccountSecretRef.Namespace,
			cluster.Spec.ServiceAccountSecretRef.Name,
			err))
	}

	// NotFound
	if secret == nil {
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidSaSecretRef,
			Status:   True,
			Reason:   NotFound,
			Category: Critical,
			Message: fmt.Sprintf("couldn't find sa secret for migcluster %s/%s",
				cluster.Spec.ServiceAccountSecretRef.Namespace,
				cluster.Spec.ServiceAccountSecretRef.Name),
		}, cluster.Namespace, cluster.Name)
		return nil
	}

	// saToken
	token, found := secret.Data[migapi.SaToken]
	if !found {
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidSaToken,
			Status:   True,
			Reason:   NotFound,
			Category: Critical,
			Message: fmt.Sprintf("couldn't find sa secret token for migcluster %s/%s",
				cluster.Spec.ServiceAccountSecretRef.Namespace,
				cluster.Spec.ServiceAccountSecretRef.Name),
		}, cluster.Namespace, cluster.Name)
		return nil
	}
	if len(token) == 0 {
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidSaToken,
			Status:   True,
			Reason:   NotSet,
			Category: Critical,
			Message: fmt.Sprintf("empty sa secret token set for migcluster %s/%s",
				cluster.Spec.ServiceAccountSecretRef.Namespace,
				cluster.Spec.ServiceAccountSecretRef.Name),
		}, cluster.Namespace, cluster.Name)
		return nil
	}

	return nil
}

// Test the connection.
func (r ReconcileMigCluster) testConnection(cluster *migapi.MigCluster) error {
	if cluster.Spec.IsHostCluster {
		log.V(4).Info("skipping connection testing as this is a host cluster",
			"name", cluster.Name, "namespace", cluster.Namespace)
		return nil
	}
	if cluster.Status.HasCriticalCondition() {
		log.V(4).Info("skipping connection testing as this cluster is in critical state",
			"name", cluster.Name, "namespace", cluster.Namespace)
		return nil
	}

	// Timeout of 5s instead of the default 30s to lessen lockup
	timeout := time.Duration(time.Second * 5)
	err := cluster.TestConnection(r.Client, timeout)
	log.V(4).Info("performing test connection for migcluster",
		"name", cluster.Name, "namespace", cluster.Namespace)
	if err != nil {
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     TestConnectFailed,
			Status:   True,
			Reason:   ConnectFailed,
			Category: Critical,
			Message:  fmt.Sprintf("test connection for migcluster failed: %s", err),
		}, cluster.Namespace, cluster.Name)
		return nil
	}

	return nil
}

func (r *ReconcileMigCluster) validateSaTokenPrivileges(cluster *migapi.MigCluster) error {
	if cluster.Spec.IsHostCluster {
		log.V(4).Info("skipping sa token privilege validation as this is a host cluster",
			"name", cluster.Name, "namespace", cluster.Namespace)
		return nil
	}
	if cluster.Status.HasCriticalCondition() {
		log.V(4).Info("skipping sa token privilege validation as this cluster is in critical state",
			"name", cluster.Name, "namespace", cluster.Namespace)
		return nil
	}

	// check for access to all verbs on all resources in all namespaces
	// in the migration.openshift.io and velero.io groups in order to
	// determine if the service account has sufficient permissions
	migrationSar := auth.SelfSubjectAccessReview{
		Spec: auth.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &auth.ResourceAttributes{
				Group:    "migration.openshift.io",
				Resource: "*",
				Verb:     "*",
				Version:  "*",
			},
		},
	}

	veleroSar := auth.SelfSubjectAccessReview{
		Spec: auth.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &auth.ResourceAttributes{
				Group:    "velero.io",
				Resource: "*",
				Verb:     "*",
				Version:  "*",
			},
		},
	}

	client, err := cluster.GetClient(r.Client)
	if err != nil {
		return liberr.Wrap(err)
	}
	err = client.Create(context.TODO(), &migrationSar)
	if err != nil {
		return liberr.Wrap(err)
	}
	err = client.Create(context.TODO(), &veleroSar)
	if err != nil {
		return liberr.Wrap(err)
	}

	switch {
	case !migrationSar.Status.Allowed:
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     SaTokenNotPrivileged,
			Status:   True,
			Reason:   Unauthorized,
			Category: Critical,
			Message:  "migration.openshift.io sa token has insufficient privileges",
		}, cluster.Namespace, cluster.Name)

	case !veleroSar.Status.Allowed:
		cluster.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     SaTokenNotPrivileged,
			Status:   True,
			Reason:   Unauthorized,
			Category: Critical,
			Message:  "velero.io sa token has insufficient privileges",
		}, cluster.Namespace, cluster.Name)
	}
	return nil
}
