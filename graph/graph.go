package graph

import (
	"fmt"
	"sync"
)

type Graph struct {
	GraphName   string
	IdAlloc       uint32
	NodeNameMapping map[string]*Node
	NodeIdMapping    map[uint32]*Node
	Next        map[uint32]map[uint32]struct{}
	InDegreeMap map[uint32]uint32
	DependOnMap map[uint32]map[uint32]struct{}
}
func (r *Graph) GetEdgeList() [][]uint32{
	var ret [][]uint32
	for from,v := range r.Next {
		for to := range v {
			ret=append(ret,[]uint32{from,to})
		}
	}
	return ret
}

func (r *Graph) FindNode(id uint32) *Node {
	return r.NodeIdMapping[id]
}
func (r *Graph) FindNodeByName(s string) *Node {
	r.initNameMapping()
	if r.NodeNameMapping != nil {
		return r.NodeNameMapping[s]
	}
	return nil
}

type Node struct {
	Id   uint32
	Name string
}

type Option func(c *Graph)

func (r *Graph) initNameMapping() {
	if len(r.NodeIdMapping) != len(r.NodeNameMapping) {
		nameMapping := make(map[string]*Node)
		for _, v := range r.NodeIdMapping {
			nameMapping[v.Name] = v
		}
		r.NodeNameMapping = nameMapping
		if len(nameMapping) != len(r.NodeIdMapping) {
			panic("has repeat Name Or Id in Node Config")
		}
	}
}

func (r *Graph) DependOn(name string, val ...string) *Graph {
	r.initNameMapping()
	nameMapping := r.NodeNameMapping
	var subs []*Node
	for _, nodeName := range val {
		h := nameMapping[nodeName]
		if h == nil {
			panic("depend on  node not found :Name=" + nodeName)
		}
		subs = append(subs, h)
	}

	r.DependOnNode(nameMapping[name], subs...)
	return r
}
func WithDependOn(to string, dependNodes ...string) Option {
	var ret []Option
	for _, from := range dependNodes {
		ret = append(ret, WithEdge(from, to))
	}
	return MergeOption(ret...)
}
func MergeOption(s ...Option) Option {
	return func(h *Graph) {
		for _, v := range s {
			v(h)
		}
	}
}
func WithEdge(from, to string) Option {
	return func(h *Graph) {
		l, r := h.FindNodeByName(from), h.FindNodeByName(to)
		if l == nil || r == nil {
			panic("not found Edge on " + from + "," + to)
		}
		h.DependOnNode(r, l)
	}
}
func WithNodes(nodes ...*Node) Option {
	return func(h *Graph) {
		for _, v := range nodes {
			h.ensureNode(v)
		}
	}
}
func NewGraph(opts ...Option) *Graph {
	var h = &Graph{
		Next:        make(map[uint32]map[uint32]struct{}),
		NodeIdMapping:    make(map[uint32]*Node),
		InDegreeMap: make(map[uint32]uint32),
	}
	for _, fn := range opts {
		fn(h)
	}
	return h
}

func (g *Graph) ensureNode(h *Node) *Node {
	if h.Id == 0 {
		for {
			g.IdAlloc++
			_, ok := g.NodeIdMapping[g.IdAlloc]
			if !ok {
				h.Id = g.IdAlloc
				break
			}
		}
	}
	_, ok := g.NodeIdMapping[h.Id]
	if !ok {
		g.NodeIdMapping[h.Id] = h
	}
	return h
}
func (r *Graph) GetDepend(id uint32) []uint32 {
	var dependList []uint32
	f := r.DependOnMap[id]
	for k := range f {
		dependList = append(dependList, k)
	}
	return dependList
}
func (g *Graph) AddNode(h *Node) {
	g.ensureNode(h)
}
func (g *Graph) DependOnNode(h *Node, depend ...*Node) {
	g.ensureNode(h)
	for _, k := range depend {
		g.ensureNode(k)
	}
	for _, pre := range depend {
		_, ok := g.Next[pre.Id][h.Id]
		if !ok {
			_, ok := g.Next[pre.Id]
			if !ok {
				g.Next[pre.Id] = make(map[uint32]struct{})
			}
			g.Next[pre.Id][h.Id] = struct{}{}
			g.InDegreeMap[h.Id]++
			if g.DependOnMap == nil {
				g.DependOnMap = make(map[uint32]map[uint32]struct{}, 0)
			}
			_, ok = g.DependOnMap[h.Id]
			if !ok {
				g.DependOnMap[h.Id] = make(map[uint32]struct{})
			}
			g.DependOnMap[h.Id][pre.Id] = struct{}{}
		}
	}
}

type TopoIterator struct {
	remaining map[uint32]struct{} // 未处理的节点集合
	inDegree  map[uint32]uint32   // 入度副本
	queue     []*Node             // 处理队列
	processed uint16              // 已处理计数
	graph     *Graph
	// start     bool
}

var (
	GraphStatePool = &Pools[*TopoIterator] {
		Pool: sync.Pool{
			New: func() any {
				return &TopoIterator{}
			},
		},
	}
)
type Pools[K any] struct {
	Pool sync.Pool
}
func (r *Pools[K]) Get() K {
	v,ok := r.Pool.Get().(K)
	if !ok {
		return *new(K)
	}
	return v
}
func (r *Pools[K]) Put(k K) {
	r.Pool.Put(k)
}

func (g *TopoIterator) Reset() {
	g.remaining = nil
	g.inDegree = nil
	g.queue = nil
	g.processed = 0
	g.graph = nil
}

func (g *Graph) TopoIterator() *TopoIterator {
	it := GraphStatePool.Get()
	// 创建入度副本
	inDegree := make(map[uint32]uint32)
	for id, d := range g.InDegreeMap {
		inDegree[id] = d
	}

	// 初始化未处理集合
	remaining := make(map[uint32]struct{})
	for id := range g.NodeIdMapping {
		remaining[id] = struct{}{}
		if _, ok := inDegree[id]; !ok {
			inDegree[id] = 0
		}
	}
	// 初始化队列
	var queue []*Node
	for id, d := range inDegree {
		if d == 0 {
			queue = append(queue, g.NodeIdMapping[id])
			delete(remaining, id)
			it.processed=1
		}
	}
	it.inDegree = inDegree
	it.remaining = remaining
	it.queue = queue
	it.graph = g
	return it
}

var (
	ErrCycleDetected = fmt.Errorf("cycle detected")
	ErrEnd           = fmt.Errorf("end of iterator")
)

func (it *TopoIterator) HasNext() bool {
	return len(it.queue) > 0 || len(it.remaining) > 0
}

func (it *TopoIterator) Next() ([]*Node, error) {
	if len(it.queue) == 0 {
		if len(it.remaining) > 0 {

			return nil, fmt.Errorf("%w at %d/%d nodes", ErrCycleDetected,
				it.processed, len(it.graph.NodeIdMapping))
		}
		// if !it.start {
		// 	return nil, ErrEnd
		// }
		return nil, nil
	}

	// 取出队列头节点
	current := it.queue[:]
	it.queue = nil
	it.processed++
	for _, current := range current {
		// 处理后继节点
		for neighborID := range it.graph.Next[current.Id] {
			it.inDegree[neighborID] = it.inDegree[neighborID] - 1
			if it.inDegree[neighborID] == 0 {
				if _, exist := it.remaining[neighborID]; exist {
					it.queue = append(it.queue, it.graph.NodeIdMapping[neighborID])
					delete(it.remaining, neighborID)
				}
			}
		}
	}

	return current, nil
}
