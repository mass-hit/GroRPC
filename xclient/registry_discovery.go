package xclient

import (
	"net/http"
	"strings"
	"time"
)

// RegistryDiscovery add registry-based service discovery support
type RegistryDiscovery struct {
	// Pointer embedding preserves shared mutable state
	*MultiServerDiscovery
	registry   string
	timeout    time.Duration
	lastUpdate time.Time
}

const defaultUpdateTimeout = time.Second * 5

func NewRegistryDiscovery(registry string, timeout time.Duration) *RegistryDiscovery {
	if timeout <= 0 {
		timeout = defaultUpdateTimeout
	}
	return &RegistryDiscovery{
		MultiServerDiscovery: NewMultiServerDiscovery(make([]string, 0)),
		registry:             registry,
		timeout:              timeout,
	}
}

func (rd *RegistryDiscovery) Update(servers []string) error {
	rd.lastUpdate = time.Now()
	return rd.MultiServerDiscovery.Update(servers)
}

// Refresh pulls the latest server list from the registry
func (rd *RegistryDiscovery) Refresh() error {
	// Use cached server list if not expired
	if time.Since(rd.lastUpdate) <= rd.timeout {
		return nil
	}
	resp, err := http.Get(rd.registry)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	servers := strings.Split(resp.Header.Get("GroRPC-Servers"), ",")
	newServers := make([]string, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			newServers = append(newServers, strings.TrimSpace(server))
		}
	}
	return rd.Update(newServers)
}

func (rd *RegistryDiscovery) Get() (string, error) {
	if err := rd.Refresh(); err != nil {
		return "", err
	}
	return rd.MultiServerDiscovery.Get()
}

func (rd *RegistryDiscovery) GetAll() ([]string, error) {
	if err := rd.Refresh(); err != nil {
		return nil, err
	}
	return rd.MultiServerDiscovery.GetAll()
}
