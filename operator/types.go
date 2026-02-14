package main

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	GroupName = "ssdkv.io"
	Version   = "v1alpha1"
	Kind      = "SsdkvCluster"
	Plural    = "ssdkvclusters"
)

// SsdkvCluster is the top-level CRD type.
type SsdkvCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SsdkvClusterSpec   `json:"spec,omitempty"`
	Status            SsdkvClusterStatus `json:"status,omitempty"`
}

// SsdkvClusterList is a list of SsdkvCluster resources.
type SsdkvClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SsdkvCluster `json:"items"`
}

// SsdkvClusterSpec defines the desired state of an SsdkvCluster.
type SsdkvClusterSpec struct {
	// Replicas is the number of nodes. Default: 3.
	Replicas *int32 `json:"replicas,omitempty"`
	// ReplicationFactor is the number of copies per key (including primary). Default: 2.
	ReplicationFactor *int32 `json:"replicationFactor,omitempty"`
	// Image is the container image for ssd-kv.
	Image string `json:"image"`
	// RedisPort is the port for Redis-compatible client connections. Default: 7777.
	RedisPort *int32 `json:"redisPort,omitempty"`
	// ClusterPort is the port for inter-node communication. Default: 7780.
	ClusterPort *int32 `json:"clusterPort,omitempty"`
	// ReplicaRead allows replica nodes to serve read requests. Default: false.
	ReplicaRead bool `json:"replicaRead,omitempty"`
	// Storage configures persistent volume for each node.
	Storage *StorageSpec `json:"storage,omitempty"`
	// Resources configures CPU/memory for each node.
	Resources *ResourceSpec `json:"resources,omitempty"`
}

// StorageSpec defines persistent storage for each node.
type StorageSpec struct {
	Size             string `json:"size,omitempty"`
	StorageClassName string `json:"storageClassName,omitempty"`
}

// ResourceSpec defines compute resources for each node.
type ResourceSpec struct {
	Requests ResourceList `json:"requests,omitempty"`
	Limits   ResourceList `json:"limits,omitempty"`
}

// ResourceList holds CPU and memory quantities.
type ResourceList struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// SsdkvClusterStatus defines the observed state.
type SsdkvClusterStatus struct {
	Phase         string `json:"phase,omitempty"`
	ReadyReplicas int32  `json:"readyReplicas,omitempty"`
}

// DeepCopyObject implements runtime.Object for SsdkvCluster.
func (in *SsdkvCluster) DeepCopyObject() runtime.Object {
	out := *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	if in.Spec.Replicas != nil {
		v := *in.Spec.Replicas
		out.Spec.Replicas = &v
	}
	if in.Spec.ReplicationFactor != nil {
		v := *in.Spec.ReplicationFactor
		out.Spec.ReplicationFactor = &v
	}
	if in.Spec.RedisPort != nil {
		v := *in.Spec.RedisPort
		out.Spec.RedisPort = &v
	}
	if in.Spec.ClusterPort != nil {
		v := *in.Spec.ClusterPort
		out.Spec.ClusterPort = &v
	}
	if in.Spec.Storage != nil {
		s := *in.Spec.Storage
		out.Spec.Storage = &s
	}
	if in.Spec.Resources != nil {
		r := *in.Spec.Resources
		out.Spec.Resources = &r
	}
	return &out
}

// DeepCopyObject implements runtime.Object for SsdkvClusterList.
func (in *SsdkvClusterList) DeepCopyObject() runtime.Object {
	out := *in
	if in.Items != nil {
		out.Items = make([]SsdkvCluster, len(in.Items))
		for i := range in.Items {
			out.Items[i] = *in.Items[i].DeepCopyObject().(*SsdkvCluster)
		}
	}
	return &out
}

// Helpers for defaults.

func (s *SsdkvClusterSpec) GetReplicas() int32 {
	if s.Replicas != nil {
		return *s.Replicas
	}
	return 3
}

func (s *SsdkvClusterSpec) GetReplicationFactor() int32 {
	if s.ReplicationFactor != nil {
		return *s.ReplicationFactor
	}
	return 2
}

func (s *SsdkvClusterSpec) GetRedisPort() int32 {
	if s.RedisPort != nil {
		return *s.RedisPort
	}
	return 7777
}

func (s *SsdkvClusterSpec) GetClusterPort() int32 {
	if s.ClusterPort != nil {
		return *s.ClusterPort
	}
	return 7780
}
