package migstorage

import (
	"fmt"
	liberr "github.com/konveyor/controller/pkg/error"
	migapi "github.com/konveyor/mig-controller/pkg/apis/migration/v1alpha1"
	migref "github.com/konveyor/mig-controller/pkg/reference"
)

// Notes:
//   BSL = Backup Storage Location
//   VSL = Volume Snapshot Location

// Types
const (
	InvalidBSProvider       = "InvalidBackupStorageProvider"
	InvalidBSCredsSecretRef = "InvalidBackupStorageCredsSecretRef"
	InvalidBSFields         = "InvalidBackupStorageSettings"
	InvalidVSProvider       = "InvalidVolumeSnapshotProvider"
	InvalidVSCredsSecretRef = "InvalidVolumeSnapshotCredsSecretRef"
	InvalidVSFields         = "InvalidVolumeSnapshotSettings"
	BSProviderTestFailed    = "BackupStorageProviderTestFailed"
	VSProviderTestFailed    = "VolumeSnapshotProviderTestFailed"
)

// Categories
const (
	Critical = migapi.Critical
)

// Reasons
const (
	Supported    = "Supported"
	NotSupported = "NotSupported"
	NotSet       = "NotSet"
	NotFound     = "NotFound"
	KeyError     = "KeyError"
	TestFailed   = "TestFailed"
)

// Statuses
const (
	True  = migapi.True
	False = migapi.False
)

// Validate the storage resource.
func (r ReconcileMigStorage) validate(storage *migapi.MigStorage) error {
	err := r.validateBackupStorage(storage)
	if err != nil {
		return liberr.Wrap(err)
	}
	err = r.validateVolumeSnapshotStorage(storage)
	if err != nil {
		return liberr.Wrap(err)
	}

	return nil
}

func (r ReconcileMigStorage) validateBackupStorage(storage *migapi.MigStorage) error {
	settings := storage.Spec.BackupStorageConfig

	if storage.Spec.BackupStorageProvider == "" {
		storage.Status.SetCondition(migapi.Condition{
			Type:     InvalidBSProvider,
			Status:   True,
			Reason:   NotSet,
			Category: Critical,
			Message:  "spec.BackupStorageProvider empty",
		})
		return nil
	}

	provider := storage.GetBackupStorageProvider()

	// Unknown provider.
	if provider == nil {
		storage.Status.SetCondition(migapi.Condition{
			Type:     InvalidBSProvider,
			Status:   True,
			Reason:   NotSupported,
			Category: Critical,
			Message:  fmt.Sprintf("couldn't get storage provider, provider: %s", storage.Spec.BackupStorageProvider),
		})
		return nil
	}

	// NotSet
	if !migref.RefSet(settings.CredsSecretRef) {
		storage.Status.SetCondition(migapi.Condition{
			Type:     InvalidBSCredsSecretRef,
			Status:   True,
			Reason:   NotSet,
			Category: Critical,
			Message:  "spec.BackupStorageConfig.CredsSecretRef name or namespace is empty",
		})
		return nil
	}

	// Secret
	secret, err := storage.GetBackupStorageCredSecret(r)
	if err != nil {
		return liberr.Wrap(fmt.Errorf("error getting migstorage backup provider secret %s/%s: %v",
			storage.Spec.BackupStorageConfig.CredsSecretRef.Namespace,
			storage.Spec.BackupStorageConfig.CredsSecretRef.Name,
			err))
	}

	// NotFound
	if secret == nil {
		storage.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidBSCredsSecretRef,
			Status:   True,
			Reason:   NotFound,
			Category: Critical,
			Message:  fmt.Sprintf("couldnt find secret for backup provider %s/%s", storage.Spec.BackupStorageConfig.CredsSecretRef.Namespace, storage.Spec.BackupStorageConfig.CredsSecretRef.Name),
		}, storage.Namespace, storage.Name)
		return nil
	}

	// Fields
	fields := provider.Validate(secret)
	if len(fields) > 0 {
		storage.Status.SetAndLogCondition(log, migapi.Condition{
			Type:     InvalidBSFields,
			Status:   True,
			Reason:   NotSupported,
			Category: Critical,
			Message:  fmt.Sprintf("secret %s/%s has invalid backing store credentials", secret.Namespace, secret.Name),
			Items:    fields,
		}, storage.Namespace, storage.Name)
		return nil
	}

	// Test provider.
	if !storage.Status.HasBlockerCondition() {
		log.V(4).Info("validateBackupStorage: testing migstorage", "name", storage.Name, "namespace", storage.Namespace)
		err = provider.Test(secret)
		if err != nil {
			storage.Status.SetAndLogCondition(log, migapi.Condition{
				Type:     BSProviderTestFailed,
				Status:   True,
				Reason:   TestFailed,
				Category: Critical,
				Message:  "Backup storage cloudprovider test failed.",
				Items:    []string{err.Error()},
			}, storage.Namespace, storage.Name)
			// TODO: raise a kube event
		}
	}

	return nil
}

func (r ReconcileMigStorage) validateVolumeSnapshotStorage(storage *migapi.MigStorage) error {
	settings := storage.Spec.VolumeSnapshotConfig

	// Provider
	provider := storage.GetVolumeSnapshotProvider()
	// Secret
	secret, err := storage.GetVolumeSnapshotCredSecret(r)
	if err != nil {
		return liberr.Wrap(fmt.Errorf("error getting migstorage backup provider secret %s/%s: %v",
			storage.Spec.VolumeSnapshotConfig.CredsSecretRef.Namespace,
			storage.Spec.VolumeSnapshotConfig.CredsSecretRef.Name,
			err))
	}

	if storage.Spec.VolumeSnapshotProvider != "" {
		// Unknown provider.
		if provider == nil {
			storage.Status.SetAndLogCondition(log, migapi.Condition{
				Type:     InvalidVSProvider,
				Status:   True,
				Reason:   NotSupported,
				Category: Critical,
				Message:  fmt.Sprintf("could not get provider for spec.VolumeSnapshotProvider: %s", storage.Spec.VolumeSnapshotProvider),
			}, storage.Namespace, storage.Name)
			return nil
		}

		// NotSet
		if !migref.RefSet(settings.CredsSecretRef) {
			storage.Status.SetAndLogCondition(log, migapi.Condition{
				Type:     InvalidVSCredsSecretRef,
				Status:   True,
				Reason:   NotSet,
				Category: Critical,
				Message:  "spec.VolumeSnapshotConfig.CredsSecretRef name or namespace is empty",
			}, storage.Namespace, storage.Name)
			return nil
		}

		// NotFound
		if secret == nil {
			storage.Status.SetAndLogCondition(log, migapi.Condition{
				Type:     InvalidVSCredsSecretRef,
				Status:   True,
				Reason:   NotFound,
				Category: Critical,
				Message: fmt.Sprintf("VolumeSnapshotConfig secret %s/%s not found",
					storage.Spec.VolumeSnapshotConfig.CredsSecretRef.Namespace,
					storage.Spec.VolumeSnapshotConfig.CredsSecretRef.Namespace),
			}, storage.Namespace, storage.Name)
			return nil
		}

		// Fields
		fields := provider.Validate(secret)
		if len(fields) > 0 {
			storage.Status.SetAndLogCondition(log, migapi.Condition{
				Type:     InvalidVSFields,
				Status:   True,
				Reason:   NotSupported,
				Category: Critical,
				Message:  fmt.Sprintf("secret %s/%s has invalid backing store credentials", secret.Namespace, secret.Name),
				Items:    fields,
			}, storage.Namespace, storage.Name)
			return nil
		}
	}

	// Test provider.
	if !storage.Status.HasBlockerCondition() {
		log.V(4).Info("validateBackupStorage: testing migstorage", "name", storage.Name, "namespace", storage.Namespace)
		err = provider.Test(secret)
		if err != nil {
			storage.Status.SetAndLogCondition(log, migapi.Condition{
				Type:     VSProviderTestFailed,
				Status:   True,
				Reason:   TestFailed,
				Category: Critical,
				Message:  "Volume Snapshot cloudprovider test failed",
				Items:    []string{err.Error()},
			}, storage.Namespace, storage.Name)
		}
	}

	return nil
}
