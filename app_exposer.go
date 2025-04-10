package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
)

// MetaInfo contains useful information provided by multiple resource types.
type MetaInfo struct {
	Name              string `json:"name"`
	Namespace         string `json:"namespace"`
	AnalysisName      string `json:"analysisName"`
	AppName           string `json:"appName"`
	AppID             string `json:"appID"`
	ExternalID        string `json:"externalID"`
	UserID            string `json:"userID"`
	Username          string `json:"username"`
	CreationTimestamp string `json:"creationTimestamp"`
}

// DeploymentInfo contains information returned about a Deployment.
type DeploymentInfo struct {
	MetaInfo
	Image   string   `json:"image"`
	Command []string `json:"command"`
	Port    int32    `json:"port"`
	User    int64    `json:"user"`
	Group   int64    `json:"group"`
}

// PodInfo tracks information about the pods for a VICE analysis.
type PodInfo struct {
	MetaInfo
	Phase                 string                   `json:"phase"`
	Message               string                   `json:"message"`
	Reason                string                   `json:"reason"`
	ContainerStatuses     []corev1.ContainerStatus `json:"containerStatuses"`
	InitContainerStatuses []corev1.ContainerStatus `json:"initContainerStatuses"`
}

// ConfigMapInfo contains useful info about a config map.
type ConfigMapInfo struct {
	MetaInfo
	Data map[string]string `json:"data"`
}

// ServiceInfoPort contains information about a service's Port.
type ServiceInfoPort struct {
	Name           string `json:"name"`
	NodePort       int32  `json:"nodePort"`
	TargetPort     int32  `json:"targetPort"`
	TargetPortName string `json:"targetPortName"`
	Port           int32  `json:"port"`
	Protocol       string `json:"protocol"`
}

// ServiceInfo contains info about a service
type ServiceInfo struct {
	MetaInfo
	Ports []ServiceInfoPort `json:"ports"`
}

// IngressInfo contains useful Ingress VICE info.
type IngressInfo struct {
	MetaInfo
	DefaultBackend string              `json:"defaultBackend"`
	Rules          []netv1.IngressRule `json:"rules"`
}

// ResourceInfo contains all of the k8s resource information about a running VICE analysis
// that we know of and care about.
type ResourceInfo struct {
	Deployments []DeploymentInfo `json:"deployments"`
	Pods        []PodInfo        `json:"pods"`
	ConfigMaps  []ConfigMapInfo  `json:"configMaps"`
	Services    []ServiceInfo    `json:"services"`
	Ingresses   []IngressInfo    `json:"ingresses"`
}

type AppExposer struct {
	baseURL *url.URL
}

// NewAppExposer returns a newly created *AppExposer
func NewAppExposer(baseURL string) (*AppExposer, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	ae := &AppExposer{
		baseURL: u,
	}

	return ae, nil
}

func (ae *AppExposer) StopAnalyses(ctx context.Context, jobID, username string) error {
	callURL := *ae.baseURL

	callURL.Path = filepath.Join(callURL.Path, "analyses", jobID, "stop")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, callURL.String(), nil)
	if err != nil {
		return err
	}

	var shortusername string
	userparts := strings.Split(username, "@")
	if len(userparts) > 1 {
		shortusername = userparts[0]
	} else {
		shortusername = username
	}
	q := req.URL.Query()
	q.Add("user", shortusername)
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("response status code for POST %s was %d as %s", callURL.String(), resp.StatusCode, username)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	log.Infof("response from %s was: %s", req.URL, string(body))
	return nil
}

func (ae *AppExposer) VICESaveAndExit(ctx context.Context, analysis *Analysis) error {
	var err error

	externalID := analysis.ExternalID

	apiURL := *ae.baseURL
	apiURL.Path = filepath.Join(apiURL.Path, "vice", externalID, "save-and-exit")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL.String(), nil)
	if err != nil {
		return errors.Wrapf(err, "error creating save-and-exit request for external-id %s", externalID)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "error calling save-and-exit for external-id %s", externalID)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("response status code for POST %s was %d", apiURL.String(), resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "error reading response body of save-and-exit call for external-id %s", externalID)
	}

	log.Infof("response from %s was: %s", req.URL, string(body))

	resp.Body.Close()

	return nil
}

func (ae *AppExposer) AdminListing(ctx context.Context, analysis *Analysis) (*ResourceInfo, error) {
	var (
		err error
		req *http.Request
	)

	api := *ae.baseURL

	api.Path = filepath.Join(api.Path, "admin", "listing")

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, api.String(), nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("external-id", analysis.ExternalID)
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("status code in response was %d", resp.StatusCode)
	}

	var resourceInfo ResourceInfo
	err = json.NewDecoder(resp.Body).Decode(&resourceInfo)
	return &resourceInfo, err
}
