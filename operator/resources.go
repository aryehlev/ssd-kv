package main

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func headlessServiceName(cluster *SsdkvCluster) string {
	return cluster.Name + "-headless"
}

func buildHeadlessService(cluster *SsdkvCluster) *corev1.Service {
	redisPort := cluster.Spec.GetRedisPort()
	clusterPort := cluster.Spec.GetClusterPort()

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(cluster),
			Namespace: cluster.Namespace,
			Labels:    labels(cluster),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  labels(cluster),
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: redisPort, TargetPort: intstr.FromInt32(redisPort), Protocol: corev1.ProtocolTCP},
				{Name: "cluster", Port: clusterPort, TargetPort: intstr.FromInt32(clusterPort), Protocol: corev1.ProtocolTCP},
			},
			PublishNotReadyAddresses: true,
		},
	}
}

func buildClientService(cluster *SsdkvCluster) *corev1.Service {
	redisPort := cluster.Spec.GetRedisPort()

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    labels(cluster),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels(cluster),
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: redisPort, TargetPort: intstr.FromInt32(redisPort), Protocol: corev1.ProtocolTCP},
			},
		},
	}
}

func buildStatefulSet(cluster *SsdkvCluster) *appsv1.StatefulSet {
	replicas := cluster.Spec.GetReplicas()
	redisPort := cluster.Spec.GetRedisPort()
	clusterPort := cluster.Spec.GetClusterPort()
	rf := cluster.Spec.GetReplicationFactor()

	// Build --cluster-peers DNS list
	headlessSvc := headlessServiceName(cluster)
	var peers []string
	for i := int32(0); i < replicas; i++ {
		// Pod DNS: <name>-<ordinal>.<headless-svc>.<namespace>.svc.cluster.local:<cluster-port>
		peers = append(peers, fmt.Sprintf("%s-%d.%s.%s.svc.cluster.local:%d",
			cluster.Name, i, headlessSvc, cluster.Namespace, clusterPort))
	}
	peerList := strings.Join(peers, ",")

	// Container command: extract ordinal from hostname
	command := []string{"/bin/sh", "-c", fmt.Sprintf(
		`ORDINAL=$(echo $HOSTNAME | rev | cut -d'-' -f1 | rev) && `+
			`exec /usr/local/bin/ssd-kv `+
			`--bind 0.0.0.0:%d `+
			`--data-dir /data `+
			`--cluster-mode `+
			`--node-id $ORDINAL `+
			`--total-nodes %d `+
			`--cluster-port %d `+
			`--cluster-peers %s `+
			`--replication-factor %d`,
		redisPort, replicas, clusterPort, peerList, rf)}

	if cluster.Spec.ReplicaRead {
		command[2] += " --replica-read"
	}

	container := corev1.Container{
		Name:    "ssdkv",
		Image:   cluster.Spec.Image,
		Command: command,
		Ports: []corev1.ContainerPort{
			{Name: "redis", ContainerPort: redisPort, Protocol: corev1.ProtocolTCP},
			{Name: "cluster", ContainerPort: clusterPort, Protocol: corev1.ProtocolTCP},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(redisPort)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(redisPort)},
			},
			InitialDelaySeconds: 15,
			PeriodSeconds:       10,
		},
	}

	// Resources
	if cluster.Spec.Resources != nil {
		container.Resources = buildResourceRequirements(cluster.Spec.Resources)
	}

	// Volume mount
	container.VolumeMounts = []corev1.VolumeMount{
		{Name: "data", MountPath: "/data"},
	}

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Labels:    labels(cluster),
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: headlessServiceName(cluster),
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels(cluster)},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(cluster)},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{container},
				},
			},
		},
	}

	// PVC template or emptyDir
	if cluster.Spec.Storage != nil && cluster.Spec.Storage.Size != "" {
		storageQty := resource.MustParse(cluster.Spec.Storage.Size)
		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "data"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: storageQty},
				},
			},
		}
		if cluster.Spec.Storage.StorageClassName != "" {
			pvc.Spec.StorageClassName = &cluster.Spec.Storage.StorageClassName
		}
		ss.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}
	} else {
		ss.Spec.Template.Spec.Volumes = []corev1.Volume{
			{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		}
	}

	return ss
}

func buildResourceRequirements(r *ResourceSpec) corev1.ResourceRequirements {
	rr := corev1.ResourceRequirements{}
	if r.Requests.CPU != "" || r.Requests.Memory != "" {
		rr.Requests = corev1.ResourceList{}
		if r.Requests.CPU != "" {
			rr.Requests[corev1.ResourceCPU] = resource.MustParse(r.Requests.CPU)
		}
		if r.Requests.Memory != "" {
			rr.Requests[corev1.ResourceMemory] = resource.MustParse(r.Requests.Memory)
		}
	}
	if r.Limits.CPU != "" || r.Limits.Memory != "" {
		rr.Limits = corev1.ResourceList{}
		if r.Limits.CPU != "" {
			rr.Limits[corev1.ResourceCPU] = resource.MustParse(r.Limits.CPU)
		}
		if r.Limits.Memory != "" {
			rr.Limits[corev1.ResourceMemory] = resource.MustParse(r.Limits.Memory)
		}
	}
	return rr
}

func labels(cluster *SsdkvCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "ssdkv",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/managed-by": "ssdkv-operator",
	}
}
