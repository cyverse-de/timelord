package main

import (
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// K8sClient handles interactions with the Kubernetes API
type K8sClient struct {
	clientset *kubernetes.Clientset
	namespace string
}

func (client *K8sClient) getDeployment(name string) (*appsv1.Deployment, error) {
	return client.clientset.AppsV1().Deployments(client.namespace).Get(name, metav1.GetOptions{})
}

func (client *K8sClient) annotateDeployment(deployment *appsv1.Deployment, key, value string) {
	annotations := deployment.GetAnnotations()
	annotations[key] = value
	deployment.SetAnnotations(annotations)
}

func (client *K8sClient) getDeploymentAnnotation(deployment *appsv1.Deployment, key string) string {
	return deployment.GetAnnotations()[key]
}

func (client *K8sClient) deploymentHasAnnotation(deployment *appsv1.Deployment, key string) bool {
	annotations := deployment.GetAnnotations()
	_, ok := annotations[key]
	return ok
}
