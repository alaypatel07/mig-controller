package directvolumemigration

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/konveyor/controller/pkg/logging"
	migapi "github.com/konveyor/mig-controller/pkg/apis/migration/v1alpha1"
	"github.com/konveyor/mig-controller/pkg/compat"
	fakecompat "github.com/konveyor/mig-controller/pkg/compat/fake"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func getTestRsyncPodForPVC(podName string, pvcName string, ns string, attemptNo string, timestamp time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.Time{
				Time: timestamp,
			},
			Name:      podName,
			Namespace: ns,
			Labels: map[string]string{
				migapi.RsyncPodIdentityLabel: pvcName,
				RsyncAttemptLabel:            attemptNo,
			},
		},
	}
}

func getTestRsyncPodWithStatusForPVC(podName string, pvcName string, ns string, attemptNo string, phase corev1.PodPhase, tstamp time.Time) *corev1.Pod {
	pod := getTestRsyncPodForPVC(podName, pvcName, ns, attemptNo, tstamp)
	pod.Status = corev1.PodStatus{
		Phase: phase,
	}
	return pod
}

func getTestRsyncOperationStatus(pvcName string, ns string, attemptNo int, succeeded bool, failed bool) *migapi.RsyncOperation {
	return &migapi.RsyncOperation{
		PVCReference: &corev1.ObjectReference{
			Name:      pvcName,
			Namespace: ns,
		},
		CurrentAttempt: attemptNo,
		Failed:         failed,
		Succeeded:      succeeded,
	}
}

func getRsyncClientPodRequirements(pvcName string, ns string) rsyncClientPodRequirements {
	fakeFsGroup := int64(0)
	fakeSupplementalGroups := []int64{1, 2}
	fakeSeLinuxOptions := corev1.SELinuxOptions{
		User: "0",
	}
	return rsyncClientPodRequirements{
		pvInfo: PVCWithSecurityContext{
			name:               pvcName,
			fsGroup:            &fakeFsGroup,
			supplementalGroups: fakeSupplementalGroups,
			seLinuxOptions:     &fakeSeLinuxOptions,
			verify:             false,
		},
		namespace:  ns,
		image:      "quay.io/konveyor/rsync-transfer:latest",
		password:   "verySecurePassword",
		privileged: false,
		resourceReq: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
		},
		nodeName: "node1.migration.internal",
		destIP:   "localhost",
		rsyncOptions: []string{
			"--partial",
			"--archive",
		},
	}
}

func arePodsEqual(p1 *corev1.Pod, p2 *corev1.Pod) bool {
	if p1 == nil && p2 == nil {
		return true
	}
	if p1 == nil || p2 == nil {
		return false
	}
	if p1.Name == p2.Name && p1.Namespace == p2.Namespace {
		return true
	}
	return false
}

func Test_hasAllRsyncClientPodsTimedOut(t *testing.T) {
	type fields struct {
		Client k8sclient.Client
		Owner  *migapi.DirectVolumeMigration
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
		{
			name: "both rsync clients pods succeeded",
			fields: fields{
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodSucceeded, ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 20}}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodSucceeded, ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 20}}},
				}),
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "both rsync clients failed with 20 seconds",
			fields: fields{
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed, ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 20}}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed, ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 20}}},
				}),
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "one rsync client pod failed with 20 second timeout other succeeded",
			fields: fields{
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodSucceeded, ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 20}}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed, ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 20}}},
				}),
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "one rsync client pod failed with 20.49 and other with 20.50 second, expect false",
			fields: fields{
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed, ContainerElapsedTime: &metav1.Duration{Duration: time.Millisecond * 20490}}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed, ContainerElapsedTime: &metav1.Duration{Duration: time.Millisecond * 20500}}},
				}),
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "one rsync client pod failed with 20.49 and other with 20.45 second, expect true",
			fields: fields{
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed, ContainerElapsedTime: &metav1.Duration{Duration: time.Millisecond * 20490}}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed, ContainerElapsedTime: &metav1.Duration{Duration: time.Millisecond * 20450}}},
				}),
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "both rsync client pods failed with nil elapsad time",
			fields: fields{
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed}},
				}),
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := Task{
				Client: tt.fields.Client,
				Owner:  tt.fields.Owner,
			}
			got, err := task.hasAllRsyncClientPodsTimedOut()
			if (err != nil) != tt.wantErr {
				t.Errorf("hasAllRsyncClientPodsTimedOut() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("hasAllRsyncClientPodsTimedOut() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isAllRsyncClientPodsNoRouteToHost(t *testing.T) {
	ten := int32(10)
	twelve := int32(12)
	type fields struct {
		Owner  *migapi.DirectVolumeMigration
		Client k8sclient.Client
	}
	tests := []struct {
		name    string
		fields  fields
		want    bool
		wantErr bool
	}{
		{
			name: "all client pods failed with no route to host",
			fields: fields{
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec: migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{
						RsyncPodStatus: migapi.RsyncPodStatus{
							PodPhase:             corev1.PodFailed,
							ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 2},
							ExitCode:             &ten,
							LogMessage: strings.Join([]string{
								"      2021/02/10 23:31:18 [1] rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"      2021/02/10 23:31:18 [1] rsync error: error in socket IO (code 10) at clientserver.c(127) [sender=3.1.3]",
								"      rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"rsync, error: error in socket IO (code 10) at clientserver.c(127) [sender = 3.1.3]"}, "\n"),
						}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec: migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{
						RsyncPodStatus: migapi.RsyncPodStatus{
							PodPhase:             corev1.PodFailed,
							ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 2},
							ExitCode:             &ten,
							LogMessage: strings.Join([]string{
								"      2021/02/10 23:31:18 [1] rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"      2021/02/10 23:31:18 [1] rsync error: error in socket IO (code 10) at clientserver.c(127) [sender=3.1.3]",
								"      rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"rsync, error: error in socket IO (code 10) at clientserver.c(127) [sender = 3.1.3]"}, "\n"),
						},
					}}),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "one client fails with exit code 10, one with 12",
			fields: fields{
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec: migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{
						RsyncPodStatus: migapi.RsyncPodStatus{
							PodPhase:             corev1.PodFailed,
							ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 2},
							ExitCode:             &ten,
							LogMessage: strings.Join([]string{
								"      2021/02/10 23:31:18 [1] rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"      2021/02/10 23:31:18 [1] rsync error: error in socket IO (code 10) at clientserver.c(127) [sender=3.1.3]",
								"      rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"rsync, error: error in socket IO (code 10) at clientserver.c(127) [sender = 3.1.3]"}, "\n"),
						}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec: migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{
						RsyncPodStatus: migapi.RsyncPodStatus{
							PodPhase:             corev1.PodFailed,
							ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 2},
							ExitCode:             &twelve,
							LogMessage: strings.Join([]string{
								"      2021/02/10 23:31:18 [1] rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"      2021/02/10 23:31:18 [1] rsync error: error in socket IO (code 10) at clientserver.c(127) [sender=3.1.3]",
								"      rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"rsync, error: error in socket IO (code 10) at clientserver.c(127) [sender = 3.1.3]"}, "\n"),
						}},
				}),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "one client fails with exit code 10, and connection reset error",
			fields: fields{
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec: migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{
						RsyncPodStatus: migapi.RsyncPodStatus{
							PodPhase:             corev1.PodFailed,
							ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 2},
							ExitCode:             &ten,
							LogMessage: strings.Join([]string{
								"      2021/02/10 23:31:18 [1] rsync: failed to connect to: connection reset error",
								"      2021/02/10 23:31:18 [1] rsync error: error in socket IO (code 10) at clientserver.c(127) [sender=3.1.3]",
								"      rsync: failed to connect to 172.30.12.121 (172.30.12.121): connection reset error (113)",
								"rsync, error: error in socket IO (code 10) at clientserver.c(127) [sender = 3.1.3]"}, "\n"),
						}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec: migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{
						RsyncPodStatus: migapi.RsyncPodStatus{
							PodPhase:             corev1.PodFailed,
							ContainerElapsedTime: &metav1.Duration{Duration: time.Second * 2},
							ExitCode:             &ten,
							LogMessage: strings.Join([]string{
								"      2021/02/10 23:31:18 [1] rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"      2021/02/10 23:31:18 [1] rsync error: error in socket IO (code 10) at clientserver.c(127) [sender=3.1.3]",
								"      rsync: failed to connect to 172.30.12.121 (172.30.12.121): No route to host (113)",
								"rsync, error: error in socket IO (code 10) at clientserver.c(127) [sender = 3.1.3]"}, "\n"),
						}},
				}),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "both rsync client pods failed with nil elapsad time",
			fields: fields{
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: migapi.DirectVolumeMigrationSpec{
						PersistentVolumeClaims: []migapi.PVCToMigrate{
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-0"}},
							{ObjectReference: &corev1.ObjectReference{Namespace: "foo", Name: "pvc-1"}},
						},
					},
				},
				Client: fake.NewFakeClient(&migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-0" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed}},
				}, &migapi.DirectVolumeMigrationProgress{
					ObjectMeta: metav1.ObjectMeta{
						Name:      getMD5Hash("test" + "pvc-1" + "foo"),
						Namespace: migapi.OpenshiftMigrationNamespace,
					},
					Spec:   migapi.DirectVolumeMigrationProgressSpec{},
					Status: migapi.DirectVolumeMigrationProgressStatus{RsyncPodStatus: migapi.RsyncPodStatus{PodPhase: corev1.PodFailed}},
				}),
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := Task{
				Owner:  tt.fields.Owner,
				Client: tt.fields.Client,
			}
			got, err := task.isAllRsyncClientPodsNoRouteToHost()
			if (err != nil) != tt.wantErr {
				t.Errorf("isAllRsyncClientPodsNoRouteToHost() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("isAllRsyncClientPodsNoRouteToHost() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTask_getLatestPodForOperation(t *testing.T) {
	type fields struct {
		Log logr.Logger
	}
	type args struct {
		client    compat.Client
		operation *migapi.RsyncOperation
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *corev1.Pod
		wantErr bool
	}{
		{
			name: "given zero rsync pods in the source namespace, should return nil",
			fields: fields{
				Log: logging.WithName("rsync-operation-test"),
			},
			args: args{
				client:    fakecompat.NewFakeClient(),
				operation: getTestRsyncOperationStatus("test-1", "test-ns", 1, false, false),
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "given one rsync pod for given pvc, should return that one pod",
			args: args{
				client: fakecompat.NewFakeClient(
					getTestRsyncPodForPVC("pod-1", "pvc-1", "ns", "1", time.Now())),
				operation: getTestRsyncOperationStatus("pvc-1", "ns", 1, false, false),
			},
			want:    getTestRsyncPodForPVC("pod-1", "pvc-1", "ns", "1", time.Now()),
			wantErr: false,
		},
		{
			name: "given more than one pods present for given pvc, should return the most recent pod",
			fields: fields{
				Log: logging.WithName("rsync-operation-test"),
			},
			args: args{
				client: fakecompat.NewFakeClient(
					getTestRsyncPodForPVC("pod-1", "pvc-1", "ns", "1", time.Now()),
					getTestRsyncPodForPVC("pod-2", "pvc-1", "ns", "2", time.Now().Add(time.Second*20)),
					getTestRsyncPodForPVC("pod-3", "pvc-1", "ns", "2", time.Now().Add(time.Second*30)),
					getTestRsyncPodForPVC("pod-4", "pvc-1", "ns", "2", time.Now().Add(time.Second*40))),
				operation: getTestRsyncOperationStatus("pvc-1", "ns", 1, false, false),
			},
			want:    getTestRsyncPodForPVC("pod-4", "pvc-1", "ns", "2", time.Now()),
			wantErr: false,
		},
		{
			name: "given more than one pods present for given pvc with some of them with non-integer attempt labels, should return the most recent pod with the correct label",
			fields: fields{
				Log: logging.WithName("rsync-operation-test"),
			},
			args: args{
				client: fakecompat.NewFakeClient(
					getTestRsyncPodForPVC("pod-1", "pvc-1", "ns", "1", time.Now()),
					getTestRsyncPodForPVC("pod-2", "pvc-1", "ns", "2", time.Now().Add(time.Second*20)),
					getTestRsyncPodForPVC("pod-3", "pvc-1", "ns", "2", time.Now().Add(time.Second*30)),
					getTestRsyncPodForPVC("pod-4", "pvc-1", "ns", "ab", time.Now().Add(time.Second*40))),
				operation: getTestRsyncOperationStatus("pvc-1", "ns", 1, false, false),
			},
			want:    getTestRsyncPodForPVC("pod-3", "pvc-1", "ns", "2", time.Now()),
			wantErr: false,
		},
		{
			name: "given more than one pods present for different PVCs, should return pod corresponding to the most recent attempt for the correct PVC",
			fields: fields{
				Log: logging.WithName("rsync-operation-test"),
			},
			args: args{
				client: fakecompat.NewFakeClient(
					getTestRsyncPodForPVC("pod-1", "pvc-1", "ns", "1", time.Now()),
					getTestRsyncPodForPVC("pod-2", "pvc-1", "ns", "2", time.Now().Add(time.Second*20)),
					getTestRsyncPodForPVC("pod-3", "pvc-2", "ns", "2", time.Now()),
					getTestRsyncPodForPVC("pod-4", "pvc-2", "ns", "2", time.Now().Add(time.Second*20))),
				operation: getTestRsyncOperationStatus("pvc-1", "ns", 1, false, false),
			},
			want:    getTestRsyncPodForPVC("pod-2", "pvc-1", "ns", "1", time.Now()),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Task{
				Log: tt.fields.Log,
			}
			got, err := tr.getLatestPodForOperation(tt.args.client, *tt.args.operation)
			if (err != nil) != tt.wantErr {
				t.Errorf("Task.getLatestPodForOperation() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !arePodsEqual(got, tt.want) {
				t.Errorf("Task.getLatestPodForOperation() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTask_ensureRsyncOperations(t *testing.T) {
	testLogr := logging.WithName("rsync-operation-test")
	type fields struct {
		Log   logr.Logger
		Owner *migapi.DirectVolumeMigration
	}
	type args struct {
		client          compat.Client
		podRequirements []rsyncClientPodRequirements
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantReturn rsyncClientOperationStatusList
		// wantPods list of pods which are expected to be present in the source ns
		wantPods []*corev1.Pod
		// dontWantPods list of pods which are not expected to be present in the source ns
		dontWantPods []*corev1.Pod
		// wantCRStatus expected CR status after one operation
		wantCRStatus []*migapi.RsyncOperation
	}{
		{
			name: "when given 0 existing Rsync pods in the source namespace and 0 new pod requirements, status list should be empty",
			args: args{
				podRequirements: []rsyncClientPodRequirements{},
				client:          fakecompat.NewFakeClient(),
			},
			fields: fields{
				Log:   testLogr,
				Owner: &migapi.DirectVolumeMigration{},
			},
			wantReturn:   rsyncClientOperationStatusList{},
			wantCRStatus: []*migapi.RsyncOperation{},
			wantPods:     []*corev1.Pod{},
			dontWantPods: []*corev1.Pod{},
		},
		{
			name: "when given 0 existing Rsync pods in the source namespace and 1 new pod requirement, 1 new pod should be created in the source namespace",
			args: args{
				podRequirements: []rsyncClientPodRequirements{
					getRsyncClientPodRequirements("111111111111111111111111111111111.11111111111111111111111111111111111111111111111111111111111111", "ns-1"),
				},
				client: fakecompat.NewFakeClient(),
			},
			fields: fields{
				Log: testLogr,
				Owner: &migapi.DirectVolumeMigration{
					Spec: migapi.DirectVolumeMigrationSpec{
						BackOffLimit: 2,
					},
				},
			},
			wantReturn: rsyncClientOperationStatusList{
				ops: []rsyncClientOperationStatus{
					{pending: true},
				},
			},
			wantCRStatus: []*migapi.RsyncOperation{
				getTestRsyncOperationStatus("111111111111111111111111111111111.11111111111111111111111111111111111111111111111111111111111111", "ns-1", 1, false, false),
			},
			wantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-1", "111111111111111111111111111111111.11111111111111111111111111111111111111111111111111111111111111 ", "ns-1", "1", time.Now()),
			},
			dontWantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-1", "111111111111111111111111111111111.11111111111111111111111111111111111111111111111111111111111111 ", "ns-1", "0", time.Now()),
				getTestRsyncPodForPVC("pod-1", "111111111111111111111111111111111.11111111111111111111111111111111111111111111111111111111111111 ", "ns-1", "2", time.Now()),
			},
		},
		{
			name: "when given 1 existing failed Rsync pod in the source namespace and backOffLimit set to 2, 1 new pod should be created in the source namespace and status should reflect correct attempt no",
			args: args{
				podRequirements: []rsyncClientPodRequirements{
					getRsyncClientPodRequirements("pvc-1", "ns-1"),
				},
				client: fakecompat.NewFakeClient(
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-1", "1", corev1.PodFailed, time.Now()),
				),
			},
			fields: fields{
				Log: testLogr,
				Owner: &migapi.DirectVolumeMigration{
					Spec: migapi.DirectVolumeMigrationSpec{
						BackOffLimit: 2,
					},
				},
			},
			wantReturn: rsyncClientOperationStatusList{
				ops: []rsyncClientOperationStatus{
					{pending: true, failed: true},
				},
			},
			wantCRStatus: []*migapi.RsyncOperation{
				getTestRsyncOperationStatus("pvc-1", "ns-1", 2, false, false),
			},
			wantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "2", time.Now()),
			},
			dontWantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "0", time.Now()),
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "3", time.Now()),
			},
		},
		{
			name: "when given 1 existing failed Rsync pod in the source namespace and backOffLimit set to 1, 1 new pod should not be created and operation should be called complete",
			args: args{
				podRequirements: []rsyncClientPodRequirements{
					getRsyncClientPodRequirements("pvc-1", "ns-1"),
				},
				client: fakecompat.NewFakeClient(
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-1", "1", corev1.PodFailed, time.Now()),
				),
			},
			fields: fields{
				Log: testLogr,
				Owner: &migapi.DirectVolumeMigration{
					Spec: migapi.DirectVolumeMigrationSpec{
						BackOffLimit: 1,
					},
				},
			},
			wantReturn: rsyncClientOperationStatusList{
				ops: []rsyncClientOperationStatus{
					{failed: true},
				},
			},
			wantCRStatus: []*migapi.RsyncOperation{
				getTestRsyncOperationStatus("pvc-1", "ns-1", 1, false, true),
			},
			wantPods: []*corev1.Pod{},
			dontWantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "0", time.Now()),
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "2", time.Now()),
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "3", time.Now()),
			},
		},
		{
			name: "when given 1 existing pending Rsync pod in the source namespace and backOffLimit set to 2, 1 new pod should not be created and operation should not be complete",
			args: args{
				podRequirements: []rsyncClientPodRequirements{
					getRsyncClientPodRequirements("pvc-1", "ns-1"),
				},
				client: fakecompat.NewFakeClient(
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-1", "1", corev1.PodPending, time.Now()),
				),
			},
			fields: fields{
				Log: testLogr,
				Owner: &migapi.DirectVolumeMigration{
					Spec: migapi.DirectVolumeMigrationSpec{
						BackOffLimit: 1,
					},
				},
			},
			wantReturn: rsyncClientOperationStatusList{
				ops: []rsyncClientOperationStatus{
					{pending: true},
				},
			},
			wantCRStatus: []*migapi.RsyncOperation{
				getTestRsyncOperationStatus("pvc-1", "ns-1", 1, false, false),
			},
			wantPods: []*corev1.Pod{},
			dontWantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "0", time.Now()),
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "2", time.Now()),
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "3", time.Now()),
			},
		},
		{
			name: "when given 3 existing failed Rsync pods in the source namespace and backOffLimit set to 4, 1 new pod should be created and operation should not be complete",
			args: args{
				podRequirements: []rsyncClientPodRequirements{
					getRsyncClientPodRequirements("pvc-1", "ns-1"),
				},
				client: fakecompat.NewFakeClient(
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-1", "1", corev1.PodFailed, time.Now()),
					getTestRsyncPodWithStatusForPVC("pod-2", "pvc-1", "ns-1", "2", corev1.PodFailed, time.Now().Add(time.Second*20)),
					getTestRsyncPodWithStatusForPVC("pod-3", "pvc-1", "ns-1", "3", corev1.PodFailed, time.Now().Add(time.Second*40)),
				),
			},
			fields: fields{
				Log: testLogr,
				Owner: &migapi.DirectVolumeMigration{
					Spec: migapi.DirectVolumeMigrationSpec{
						BackOffLimit: 4,
					},
				},
			},
			wantReturn: rsyncClientOperationStatusList{
				ops: []rsyncClientOperationStatus{
					{pending: true, failed: true},
				},
			},
			wantCRStatus: []*migapi.RsyncOperation{
				getTestRsyncOperationStatus("pvc-1", "ns-1", 4, false, false),
			},
			wantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-4", "pvc-1", "ns-1", "4", time.Now()),
			},
			dontWantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "0", time.Now()),
				getTestRsyncPodForPVC("pod-1", "pvc-1", "ns-1", "5", time.Now()),
			},
		},
		{
			name: "when given 3 different failed Rsync pods for 3 different pvcs and backOffLimit set to 2, 1 new pod should be created each pvc and operation should not be complete",
			args: args{
				podRequirements: []rsyncClientPodRequirements{
					getRsyncClientPodRequirements("pvc-1", "ns-1"),
					getRsyncClientPodRequirements("pvc-1", "ns-2"),
					getRsyncClientPodRequirements("pvc-1", "ns-3"),
				},
				client: fakecompat.NewFakeClient(
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-1", "1", corev1.PodFailed, time.Now()),
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-2", "1", corev1.PodFailed, time.Now()),
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-3", "1", corev1.PodFailed, time.Now()),
				),
			},
			fields: fields{
				Log: testLogr,
				Owner: &migapi.DirectVolumeMigration{
					Spec: migapi.DirectVolumeMigrationSpec{
						BackOffLimit: 2,
					},
				},
			},
			wantReturn: rsyncClientOperationStatusList{
				ops: []rsyncClientOperationStatus{
					{pending: true, failed: true},
					{pending: true, failed: true},
					{pending: true, failed: true},
				},
			},
			wantCRStatus: []*migapi.RsyncOperation{
				getTestRsyncOperationStatus("pvc-1", "ns-1", 2, false, false),
				getTestRsyncOperationStatus("pvc-1", "ns-2", 2, false, false),
				getTestRsyncOperationStatus("pvc-1", "ns-3", 2, false, false),
			},
			wantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-1", "2", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-2", "2", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-3", "2", time.Now()),
			},
			dontWantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-1", "0", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-2", "0", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-3", "0", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-1", "3", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-2", "3", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-3", "3", time.Now()),
			},
		},
		{
			name: "when given different Rsync pods with mix of different statuses for 10 different pvcs and backOffLimit set to 5, should return correct statuses for each pvc",
			args: args{
				podRequirements: []rsyncClientPodRequirements{
					getRsyncClientPodRequirements("pvc-1", "ns-1"),
					getRsyncClientPodRequirements("pvc-2", "ns-1"),
					getRsyncClientPodRequirements("pvc-1", "ns-2"),
					getRsyncClientPodRequirements("pvc-2", "ns-2"),
					getRsyncClientPodRequirements("pvc-1", "ns-3"),
					getRsyncClientPodRequirements("pvc-2", "ns-3"),
					getRsyncClientPodRequirements("pvc-3", "ns-3"),
					getRsyncClientPodRequirements("pvc-4", "ns-3"),
					getRsyncClientPodRequirements("pvc-5", "ns-3"),
					getRsyncClientPodRequirements("pvc-6", "ns-3"),
				},
				client: fakecompat.NewFakeClient(
					// pods for ns-1/pvc-1
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-1", "1", corev1.PodFailed, time.Now()),
					getTestRsyncPodWithStatusForPVC("pod-2", "pvc-1", "ns-1", "2b", corev1.PodUnknown, time.Now().Add(time.Second*5)),
					getTestRsyncPodWithStatusForPVC("pod-3", "pvc-1", "ns-1", "3", corev1.PodFailed, time.Now().Add(time.Second*10)),
					// pods for ns-1/pvc-2
					getTestRsyncPodWithStatusForPVC("pod-4", "pvc-2", "ns-1", "1", corev1.PodFailed, time.Now()),
					getTestRsyncPodWithStatusForPVC("pod-5", "pvc-2", "ns-1", "2", corev1.PodUnknown, time.Now().Add(time.Second*5)),
					getTestRsyncPodWithStatusForPVC("pod-6", "pvc-2", "ns-1", "3", corev1.PodFailed, time.Now().Add(time.Second*10)),
					getTestRsyncPodWithStatusForPVC("pod-7", "pvc-2", "ns-1", "4", corev1.PodFailed, time.Now().Add(time.Second*15)),
					getTestRsyncPodWithStatusForPVC("pod-8", "pvc-2", "ns-1", "5", corev1.PodPending, time.Now().Add(time.Second*25)),
					// pods for ns-2/pvc-1
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-2", "1", corev1.PodFailed, time.Now()),
					getTestRsyncPodWithStatusForPVC("pod-2", "pvc-1", "ns-2", "2", corev1.PodUnknown, time.Now().Add(time.Second*5)),
					getTestRsyncPodWithStatusForPVC("pod-3", "pvc-1", "ns-2", "3", corev1.PodSucceeded, time.Now().Add(time.Second*10)),
					// pods for ns-2/pvc-2
					getTestRsyncPodWithStatusForPVC("pod-4", "pvc-2", "ns-2", "1", corev1.PodFailed, time.Now()),
					getTestRsyncPodWithStatusForPVC("pod-5", "pvc-2", "ns-2", "2", corev1.PodUnknown, time.Now().Add(time.Second*5)),
					getTestRsyncPodWithStatusForPVC("pod-6", "pvc-2", "ns-2", "3", corev1.PodFailed, time.Now().Add(time.Second*10)),
					getTestRsyncPodWithStatusForPVC("pod-7", "pvc-2", "ns-2", "4", corev1.PodFailed, time.Now().Add(time.Second*15)),
					getTestRsyncPodWithStatusForPVC("pod-8", "pvc-2", "ns-2", "5", corev1.PodFailed, time.Now().Add(time.Second*20)),
					// pods for ns-3/pvc-1
					getTestRsyncPodWithStatusForPVC("pod-1", "pvc-1", "ns-3", "5", corev1.PodSucceeded, time.Now().Add(time.Second*20)),
					//.pods for ns-3/pvc-2
					getTestRsyncPodWithStatusForPVC("pod-2", "pvc-2", "ns-3", "1", corev1.PodRunning, time.Now().Add(time.Second*20)),
					//.no pods for ns-3/pvc-3 ns-3/pvc-4 ns-3/pvc-5 ns-3/pvc-6
				),
			},
			fields: fields{
				Log: testLogr,
				Owner: &migapi.DirectVolumeMigration{
					Spec: migapi.DirectVolumeMigrationSpec{
						BackOffLimit: 5,
					},
				},
			},
			wantReturn: rsyncClientOperationStatusList{
				ops: []rsyncClientOperationStatus{
					{pending: true, failed: true},
					{pending: true},
					{succeeded: true},
					{failed: true},
					{succeeded: true},
					{running: true},
					{pending: true},
					{pending: true},
					{pending: true},
					{pending: true},
				},
			},
			wantCRStatus: []*migapi.RsyncOperation{
				getTestRsyncOperationStatus("pvc-1", "ns-1", 4, false, false),
				getTestRsyncOperationStatus("pvc-2", "ns-1", 5, false, false),
				getTestRsyncOperationStatus("pvc-1", "ns-2", 3, true, false),
				getTestRsyncOperationStatus("pvc-2", "ns-2", 5, false, true),
				getTestRsyncOperationStatus("pvc-1", "ns-3", 5, true, false),
				getTestRsyncOperationStatus("pvc-2", "ns-3", 1, false, false),
				getTestRsyncOperationStatus("pvc-3", "ns-3", 1, false, false),
				getTestRsyncOperationStatus("pvc-4", "ns-3", 1, false, false),
				getTestRsyncOperationStatus("pvc-5", "ns-3", 1, false, false),
				getTestRsyncOperationStatus("pvc-6", "ns-3", 1, false, false),
			},
			wantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-1", "4", time.Now()),
			},
			dontWantPods: []*corev1.Pod{
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-1", "0", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-1", "5", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-2", "0", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-2", "4", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-2", "ns-2", "0", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-2", "ns-2", "6", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-1", "ns-3", "2", time.Now()),
				getTestRsyncPodForPVC("pod-2", "pvc-2", "ns-3", "2", time.Now()),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Task{
				Log:   tt.fields.Log,
				Owner: tt.fields.Owner,
			}
			got, _ := tr.ensureRsyncOperations(tt.args.client, tt.args.podRequirements)
			// check whether the returned value matches the expectations
			if got.Succeeded() != tt.wantReturn.Succeeded() {
				t.Errorf("RsyncOperationsContext.EnsureRsyncOperations() = got %d succeded operations, want %d", got.Succeeded(), tt.wantReturn.Succeeded())
			}
			if got.Pending() != tt.wantReturn.Pending() {
				t.Errorf("RsyncOperationsContext.EnsureRsyncOperations() = got %d pending operations, want %d", got.Pending(), tt.wantReturn.Pending())
			}
			if got.Failed() != tt.wantReturn.Failed() {
				t.Errorf("RsyncOperationsContext.EnsureRsyncOperations() = got %d failed operations, want %d", got.Failed(), tt.wantReturn.Failed())
			}
			if got.Running() != tt.wantReturn.Running() {
				t.Errorf("RsyncOperationsContext.EnsureRsyncOperations() = got %d running operations, want %d", got.Running(), tt.wantReturn.Running())
			}

			// check whether the updated CR status matches the expectations
			for _, s := range tt.wantCRStatus {
				got := tt.fields.Owner.Status.GetRsyncOperationStatusForPVC(&corev1.ObjectReference{
					Name:      s.PVCReference.Name,
					Namespace: s.PVCReference.Namespace,
				})
				if !reflect.DeepEqual(*got, *s) {
					t.Errorf("RsyncOperationsContext.EnsureRsyncOperations() expected operation status doesnt match actual, want %v got %v",
						*s, *got)
				}
			}
			podExistsInSource := func(pod *corev1.Pod) bool {
				srcPods := corev1.PodList{}
				errs := tt.args.client.List(context.TODO(), k8sclient.InNamespace(pod.Namespace).MatchingLabels(pod.Labels), &srcPods)
				if errs != nil {
					t.Errorf("RsyncOperationsContext.EnsureRsyncOperations() failed getting pods in source namespace")
				}
				return len(srcPods.Items) > 0
			}
			// check whether expected pods are present in the source ns
			for _, pod := range tt.wantPods {
				if !podExistsInSource(pod) {
					t.Errorf("RsyncOperationsContext.EnsureRsyncOperations() expected pod with labels %v not found in the source ns", pod.Labels)
				}
			}
			// check whether unexpected pods are not present in the source ns
			for _, pod := range tt.dontWantPods {
				if podExistsInSource(pod) {
					t.Errorf("RsyncOperationsContext.EnsureRsyncOperations() unexpected pod with labels %v found in the source ns", pod.Labels)
				}
			}
		})
	}
}

func TestTask_processRsyncOperationStatus(t *testing.T) {
	type fields struct {
		Log    logr.Logger
		Client k8sclient.Client
		Owner  *migapi.DirectVolumeMigration
	}
	type args struct {
		status                  rsyncClientOperationStatusList
		garbageCollectionErrors []error
	}
	tests := []struct {
		name              string
		fields            fields
		args              args
		wantAllCompleted  bool
		wantAnyFailed     bool
		wantErr           bool
		wantCondition     *migapi.Condition
		dontWantCondition *migapi.Condition
	}{
		{
			name: "when there are errors within less than 5 minutes of running the phase, warning should not be present",
			fields: fields{
				Log:    log.WithName("test-logger"),
				Client: fake.NewFakeClient(),
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-dvm", Namespace: "openshift-migration",
					},
					Status: migapi.DirectVolumeMigrationStatus{
						Conditions: migapi.Conditions{
							List: []migapi.Condition{
								{Type: Running, Category: Advisory, Status: True, LastTransitionTime: metav1.Time{Time: time.Now().Add(-1 * time.Minute)}},
							},
						},
					},
				},
			},
			args: args{
				status: rsyncClientOperationStatusList{
					ops: []rsyncClientOperationStatus{
						{errors: []error{fmt.Errorf("failed creating pod")}},
					},
				},
			},
			wantAllCompleted:  false,
			wantAnyFailed:     false,
			wantCondition:     nil,
			dontWantCondition: &migapi.Condition{Type: FailedCreatingRsyncPods, Status: True, Category: Warn},
		},
		{
			name: "when there are errors for over 5 minutes of running the phase, warning should be present",
			fields: fields{
				Log:    log.WithName("test-logger"),
				Client: fake.NewFakeClient(),
				Owner: &migapi.DirectVolumeMigration{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-dvm", Namespace: "openshift-migration",
					},
					Status: migapi.DirectVolumeMigrationStatus{
						Conditions: migapi.Conditions{
							List: []migapi.Condition{
								{Type: Running, Category: Advisory, Status: True, LastTransitionTime: metav1.Time{Time: time.Now().Add(-6 * time.Minute)}},
							},
						},
					},
				},
			},
			args: args{
				status: rsyncClientOperationStatusList{
					ops: []rsyncClientOperationStatus{
						{errors: []error{fmt.Errorf("failed creating pod")}},
					},
				},
			},
			wantAllCompleted:  false,
			wantAnyFailed:     false,
			wantCondition:     &migapi.Condition{Type: FailedCreatingRsyncPods, Status: True, Category: Warn},
			dontWantCondition: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Task{
				Log:    tt.fields.Log,
				Client: tt.fields.Client,
				Owner:  tt.fields.Owner,
			}
			gotAllCompleted, gotAnyFailed, _, err := tr.processRsyncOperationStatus(tt.args.status, tt.args.garbageCollectionErrors)
			if (err != nil) != tt.wantErr {
				t.Errorf("Task.processRsyncOperationStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotAllCompleted != tt.wantAllCompleted {
				t.Errorf("Task.processRsyncOperationStatus() gotAllCompleted = %v, want %v", gotAllCompleted, tt.wantAllCompleted)
			}
			if gotAnyFailed != tt.wantAnyFailed {
				t.Errorf("Task.processRsyncOperationStatus() gotAnyFailed = %v, want %v", gotAnyFailed, tt.wantAnyFailed)
			}
			if tt.wantCondition != nil && !tt.fields.Owner.Status.HasCondition(tt.wantCondition.Type) {
				t.Errorf("Task.processRsyncOperationStatus() didn't find expected condition of type %s", tt.wantCondition.Type)
			}
			if tt.dontWantCondition != nil && tt.fields.Owner.Status.HasCondition(tt.dontWantCondition.Type) {
				t.Errorf("Task.processRsyncOperationStatus() found unexpected condition of type %s", tt.dontWantCondition.Type)
			}
		})
	}
}

func Test_getDNSSafeName(t *testing.T) {
	tests := []struct {
		name       string
		volumeName string
		want       string
		wantErr    bool
	}{
		{
			name:       "valid name",
			volumeName: "valid",
			want:       "valid",
			wantErr:    false,
		},
		{
			name:       "longer than 63 chars",
			volumeName: strings.Repeat("1", 64),
			want:       strings.Repeat("1", 63),
			wantErr:    false,
		},
		{
			name:       "valid length but invalid characters",
			volumeName: "11111.11111%11111/11111",
			want:       "11111-11111-11111-11111",
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getDNSSafeName(tt.volumeName)
			if (err != nil) != tt.wantErr {
				t.Errorf("getDNSSafeName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getDNSSafeName() got = %v, want %v", got, tt.want)
			}
		})
	}
}
