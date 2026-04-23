package main

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// SsdkvClusterReconciler reconciles SsdkvCluster objects.
type SsdkvClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *SsdkvClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the SsdkvCluster resource
	var cluster SsdkvCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("SsdkvCluster deleted", "name", req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("Reconciling SsdkvCluster", "name", cluster.Name, "replicas", cluster.Spec.GetReplicas())

	// 1. Headless Service
	headlessSvc := buildHeadlessService(&cluster)
	if err := r.reconcileService(ctx, &cluster, headlessSvc); err != nil {
		return ctrl.Result{}, fmt.Errorf("headless service: %w", err)
	}

	// 2. Client Service
	clientSvc := buildClientService(&cluster)
	if err := r.reconcileService(ctx, &cluster, clientSvc); err != nil {
		return ctrl.Result{}, fmt.Errorf("client service: %w", err)
	}

	// 3. StatefulSet
	desiredSS, err := buildStatefulSet(&cluster)
	if err != nil {
		cluster.Status.Phase = "Invalid"
		_ = r.Status().Update(ctx, &cluster)
		return ctrl.Result{}, fmt.Errorf("build statefulset: %w", err)
	}
	if err := r.reconcileStatefulSet(ctx, &cluster, desiredSS); err != nil {
		return ctrl.Result{}, fmt.Errorf("statefulset: %w", err)
	}

	// 4. Update status
	var currentSS appsv1.StatefulSet
	if err := r.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, &currentSS); err == nil {
		cluster.Status.ReadyReplicas = currentSS.Status.ReadyReplicas
		if currentSS.Status.ReadyReplicas == cluster.Spec.GetReplicas() {
			cluster.Status.Phase = "Running"
		} else if currentSS.Status.ReadyReplicas > 0 {
			cluster.Status.Phase = "Degraded"
		} else {
			cluster.Status.Phase = "Pending"
		}
	}

	return ctrl.Result{}, nil
}

func (r *SsdkvClusterReconciler) reconcileService(ctx context.Context, owner *SsdkvCluster, desired *corev1.Service) error {
	if err := controllerutil.SetControllerReference(owner, desired, r.Scheme); err != nil {
		return err
	}

	var existing corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update ports (preserve ClusterIP)
	existing.Spec.Ports = desired.Spec.Ports
	existing.Spec.Selector = desired.Spec.Selector
	return r.Update(ctx, &existing)
}

func (r *SsdkvClusterReconciler) reconcileStatefulSet(ctx context.Context, owner *SsdkvCluster, desired *appsv1.StatefulSet) error {
	if err := controllerutil.SetControllerReference(owner, desired, r.Scheme); err != nil {
		return err
	}

	var existing appsv1.StatefulSet
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing)
	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update mutable fields
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.Template = desired.Spec.Template
	return r.Update(ctx, &existing)
}

// SetupWithManager registers the controller with the manager.
func (r *SsdkvClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Register CRD types with scheme
	schemeBuilder := runtime.NewSchemeBuilder(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(
			SchemeGroupVersion,
			&SsdkvCluster{},
			&SsdkvClusterList{},
		)
		metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
		return nil
	})
	if err := schemeBuilder.AddToScheme(mgr.GetScheme()); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&SsdkvCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
