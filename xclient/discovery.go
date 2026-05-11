package xclient

import (
	"errors"
	"sync"
	"sync/atomic"
)

type Discovery interface {
	Update([]string) error
	Get() (string, error)
	GetAll() ([]string, error)
}

// MultiServerDiscovery manages multiple RPC server addresses
type MultiServerDiscovery struct {
	mu        sync.RWMutex // protects servers slice
	servers   []string
	weightMap sync.Map // map[string]*weightRoundRobin
}

func NewMultiServerDiscovery(servers []string) *MultiServerDiscovery {
	m := &MultiServerDiscovery{
		servers: servers,
	}
	for _, server := range servers {
		wrr := &weightRoundRobin{}
		wrr.weight.Store(1)
		m.weightMap.Store(server, wrr)
	}
	return m
}

// weightRoundRobin stores weighted round-robin state
type weightRoundRobin struct {
	weight  atomic.Int32 // Default server weight = 1
	current atomic.Int32
}

// SetWeight updates weight of server
func (d *MultiServerDiscovery) SetWeight(server string, weight int32) error {
	if weight <= 0 {
		return errors.New("weight must greater than 0")
	}
	var found bool
	d.mu.RLock()
	for _, s := range d.servers {
		if s == server {
			found = true
			break
		}
	}
	d.mu.RUnlock()
	if !found {
		return errors.New("service not found")
	}
	val, ok := d.weightMap.Load(server)
	if !ok {
		return errors.New("service not found")
	}
	wrr, _ := val.(*weightRoundRobin)
	wrr.weight.Store(weight)
	wrr.current.Store(0)
	return nil
}

// Update replaces the current server list
func (d *MultiServerDiscovery) Update(servers []string) error {
	d.mu.Lock()
	oldMap := make(map[string]bool)
	for _, server := range d.servers {
		oldMap[server] = true
	}
	newMap := make(map[string]bool)
	for _, server := range servers {
		newMap[server] = true
	}
	d.servers = servers
	d.mu.Unlock()
	for server := range oldMap {
		if !newMap[server] {
			d.weightMap.Delete(server)
		}
	}
	for server := range newMap {
		if _, ok := d.weightMap.Load(server); !ok {
			wrr := &weightRoundRobin{}
			wrr.weight.Store(1)
			d.weightMap.Store(server, wrr)
		}
	}
	return nil
}

// Get selects a server using smooth weighted round-robin
func (d *MultiServerDiscovery) Get() (string, error) {
	d.mu.RLock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("no servers")
	}
	servers := d.servers
	d.mu.RUnlock()
	totalWeight, maxCur := 0, 0
	var selectedWRR *weightRoundRobin
	var selected string
	for _, server := range servers {
		value, ok := d.weightMap.Load(server)
		if !ok {
			continue
		}
		wrr := value.(*weightRoundRobin)
		weight := wrr.weight.Load()
		// current += weight
		cur := wrr.current.Add(weight)
		totalWeight += int(weight)
		// Select server with the largest current
		if int(cur) > maxCur {
			maxCur = int(cur)
			selectedWRR = wrr
			selected = server
		}
	}
	if selectedWRR != nil {
		// selected.current -= totalWeight
		selectedWRR.current.Add(int32(totalWeight * -1))
		return selected, nil
	}
	return "", errors.New("no servers")
}

// GetAll returns a copy of all servers
func (d *MultiServerDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}
