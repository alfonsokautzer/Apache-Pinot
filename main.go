package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Segment represents a data segment with a range of records.
type Segment struct {
	ID        string
	RecordCnt int64 // accessed atomically
}

// ErrSegmentNotPresent is returned when a node does not have the segment.
var ErrSegmentNotPresent = errors.New("SegmentNotPresentException")

// Node represents a server (Ingestion or Historical) storing segments.
type Node struct {
	ID       string
	mu       sync.RWMutex
	segments map[string]*Segment
}

func NewNode(id string) *Node {
	return &Node{
		ID:       id,
		segments: make(map[string]*Segment),
	}
}

func (n *Node) LoadSegment(seg *Segment) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.segments[seg.ID] = seg
	fmt.Printf("[Node %s] Loaded segment %s with %d records\n", n.ID, seg.ID, atomic.LoadInt64(&seg.RecordCnt))
}

func (n *Node) DropSegment(segID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.segments, segID)
	fmt.Printf("[Node %s] Dropped segment %s\n", n.ID, segID)
}

func (n *Node) Query(segID string) (int64, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	seg, exists := n.segments[segID]
	if !exists {
		return 0, ErrSegmentNotPresent
	}
	return atomic.LoadInt64(&seg.RecordCnt), nil
}

// MetadataStore simulates ZooKeeper/Consul.
type MetadataStore struct {
	mu       sync.RWMutex
	state    map[string][]string // segmentID -> list of nodeIDs that have it loaded
	watchers []chan struct{}
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		state: make(map[string][]string),
	}
}

func (m *MetadataStore) UpdateSegmentState(segID string, nodeID string, loaded bool) {
	m.mu.Lock()
	nodes := m.state[segID]
	if loaded {
		exists := false
		for _, n := range nodes {
			if n == nodeID {
				exists = true
				break
			}
		}
		if !exists {
			m.state[segID] = append(nodes, nodeID)
		}
	} else {
		newNodes := []string{}
		for _, n := range nodes {
			if n != nodeID {
				newNodes = append(newNodes, n)
			}
		}
		m.state[segID] = newNodes
	}
	watchers := m.watchers
	m.watchers = nil
	m.mu.Unlock()

	// Notify watchers
	for _, ch := range watchers {
		close(ch)
	}
}

func (m *MetadataStore) GetSegmentLocations(segID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := m.state[segID]
	copied := make([]string, len(nodes))
	copy(copied, nodes)
	return copied
}

func (m *MetadataStore) Watch() chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan struct{})
	m.watchers = append(m.watchers, ch)
	return ch
}

// Broker routes queries to nodes.
type Broker struct {
	mu           sync.RWMutex
	routingTable map[string][]string // segmentID -> nodeIDs
	nodes        map[string]*Node
	metaStore    *MetadataStore
}

func NewBroker(ctx context.Context, metaStore *MetadataStore, nodes map[string]*Node) *Broker {
	b := &Broker{
		routingTable: make(map[string][]string),
		nodes:        nodes,
		metaStore:    metaStore,
	}
	go b.syncLoop(ctx)
	return b
}

func (b *Broker) syncLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		ch := b.metaStore.Watch()
		b.syncRoutingTable()
		select {
		case <-ctx.Done():
			return
		case <-ch:
		}
	}
}

func (b *Broker) syncRoutingTable() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.metaStore.mu.RLock()
	defer b.metaStore.mu.RUnlock()
	for segID, nodes := range b.metaStore.state {
		copied := make([]string, len(nodes))
		copy(copied, nodes)
		b.routingTable[segID] = copied
	}
}

func (b *Broker) QueryAll() (int64, error) {
	b.mu.RLock()
	segIDs := make([]string, 0, len(b.routingTable))
	for segID := range b.routingTable {
		segIDs = append(segIDs, segID)
	}
	b.mu.RUnlock()

	var total int64
	for _, segID := range segIDs {
		val, err := b.QuerySegmentWithRetry(segID)
		if err != nil {
			return 0, err
		}
		total += val
	}
	return total, nil
}

func (b *Broker) QuerySegmentWithRetry(segID string) (int64, error) {
	retries := 3
	for i := 0; i < retries; i++ {
		b.mu.RLock()
		nodes := b.routingTable[segID]
		b.mu.RUnlock()

		if len(nodes) == 0 {
			// Fast-path metadata sync
			b.syncRoutingTable()
			b.mu.RLock()
			nodes = b.routingTable[segID]
			b.mu.RUnlock()
			if len(nodes) == 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		// Try routing to the first available node
		for _, nodeID := range nodes {
			node := b.nodes[nodeID]
			val, err := node.Query(segID)
			if err == nil {
				return val, nil
			}
			if errors.Is(err, ErrSegmentNotPresent) {
				fmt.Printf("[Broker] SegmentNotPresentException for %s on %s, refreshing routing table and retrying...\n", segID, nodeID)
				b.syncRoutingTable()
				break // break inner loop to retry with updated routing table
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("failed to query segment %s after retries", segID)
}

// Coordinator manages segment movement.
type Coordinator struct {
	metaStore *MetadataStore
	nodes     map[string]*Node
	mu        sync.Mutex
	active    map[string]bool
}

func NewCoordinator(metaStore *MetadataStore, nodes map[string]*Node) *Coordinator {
	return &Coordinator{
		metaStore: metaStore,
		nodes:     nodes,
		active:    make(map[string]bool),
	}
}

// RebalanceSegment moves a segment from source to destination using Load-Before-Drop protocol.
func (c *Coordinator) RebalanceSegment(seg *Segment, srcNodeID, destNodeID string) {
	c.mu.Lock()
	if c.active[seg.ID] {
		c.mu.Unlock()
		return
	}
	c.active[seg.ID] = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.active, seg.ID)
		c.mu.Unlock()
	}()

	fmt.Printf("[Coordinator] Starting rebalance of segment %s from %s to %s\n", seg.ID, srcNodeID, destNodeID)

	// 1. Load segment on destination node
	destNode := c.nodes[destNodeID]
	destNode.LoadSegment(seg)

	// 2. Advertise loaded state in Metadata Store
	c.metaStore.UpdateSegmentState(seg.ID, destNodeID, true)

	// 3. Wait for Broker to update its routing table and ensure destination is active
	time.Sleep(20 * time.Millisecond)

	// 4. Drop segment from source node
	srcNode := c.nodes[srcNodeID]
	srcNode.DropSegment(seg.ID)

	// 5. Update Metadata Store to remove source node
	c.metaStore.UpdateSegmentState(seg.ID, srcNodeID, false)
	fmt.Printf("[Coordinator] Completed rebalance of segment %s\n", seg.ID)
}

func main() {
	// 1. Start a minified cluster (1 Broker, 1 Coordinator, 2 Historicals, 1 Ingestion node)
	ingestionNode := NewNode("Ingestion")
	historical1 := NewNode("Historical1")
	historical2 := NewNode("Historical2")

	nodes := map[string]*Node{
		"Ingestion":   ingestionNode,
		"Historical1": historical1,
		"Historical2": historical2,
	}

	metaStore := NewMetadataStore()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	broker := NewBroker(ctx, metaStore, nodes)
	coordinator := NewCoordinator(metaStore, nodes)

	var recordCount int64
	var segmentIDCounter int64
	var currentSegment *Segment
	var mu sync.Mutex

	// 2. Start a continuous ingestion stream writing records
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond) // 100 records/sec (1 record every 10ms)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				if currentSegment == nil || atomic.LoadInt64(&currentSegment.RecordCnt) >= 50 {
					// Create a new segment
					segID := fmt.Sprintf("seg_%d", atomic.AddInt64(&segmentIDCounter, 1))
					currentSegment = &Segment{ID: segID, RecordCnt: 0}
					ingestionNode.LoadSegment(currentSegment)
					metaStore.UpdateSegmentState(segID, "Ingestion", true)
				}
				atomic.AddInt64(&currentSegment.RecordCnt, 1)
				atomic.AddInt64(&recordCount, 1)
				mu.Unlock()
			}
		}
	}()

	// Wait for some data to be ingested
	time.Sleep(500 * time.Millisecond)

	// 3. Run a background thread executing aggregate queries and asserting count is monotonically increasing
	var lastCount int64
	var queryFailures int64
	var countDrops int64
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count, err := broker.QueryAll()
				if err != nil {
					fmt.Printf("[Query Error] %v\n", err)
					atomic.AddInt64(&queryFailures, 1)
					continue
				}
				// Assert count is monotonically increasing
				prev := atomic.LoadInt64(&lastCount)
				if count < prev {
					fmt.Printf("[Assertion Failed] Count dropped from %d to %d\n", prev, count)
					atomic.AddInt64(&countDrops, 1)
				} else {
					atomic.StoreInt64(&lastCount, count)
				}
			}
		}
	}()

	// 4. Trigger manual rebalance or segment movement between nodes
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Move segments from Ingestion to Historical1, or Historical1 to Historical2
				metaStore.mu.RLock()
				var segsToMove []string
				for segID := range metaStore.state {
					segsToMove = append(segsToMove, segID)
				}
				metaStore.mu.RUnlock()

				for _, segID := range segsToMove {
					locations := metaStore.GetSegmentLocations(segID)
					if len(locations) == 0 {
						continue
					}
					src := locations[0]
					var dest string
					if src == "Ingestion" {
						dest = "Historical1"
					} else if src == "Historical1" {
						dest = "Historical2"
					} else {
						dest = "Historical1"
					}

					// Fetch segment object
					mu.Lock()
					var seg *Segment
					if currentSegment != nil && currentSegment.ID == segID {
						seg = currentSegment
					} else {
						// Find segment in source node
						srcNode := nodes[src]
						srcNode.mu.RLock()
						seg = srcNode.segments[segID]
						srcNode.mu.RUnlock()
					}
					mu.Unlock()

					if seg != nil {
						go coordinator.RebalanceSegment(seg, src, dest)
					}
				}
			}
		}
	}()

	// Run simulation for 4 seconds
	time.Sleep(4 * time.Second)
	cancel()
	wg.Wait()

	fmt.Println("--- Simulation Results ---")
	fmt.Printf("Total Ingested Records: %d\n", atomic.LoadInt64(&recordCount))
	fmt.Printf("Last Query Count: %d\n", atomic.LoadInt64(&lastCount))
	fmt.Printf("Query Failures: %d\n", atomic.LoadInt64(&queryFailures))
	fmt.Printf("Count Drops (Data Holes): %d\n", atomic.LoadInt64(&countDrops))

	if atomic.LoadInt64(&queryFailures) > 0 || atomic.LoadInt64(&countDrops) > 0 {
		fmt.Println("Simulation FAILED: Race conditions detected!")
	} else {
		fmt.Println("Simulation PASSED: Zero queries failed or returned decreased count during rebalance.")
	}
}