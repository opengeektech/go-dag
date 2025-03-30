package dag

import (
	"github.com/opengeektech/go-dag/graph"
)

type nodeHelper struct {
	g          *graph.Graph
	pending    map[uint32]struct{}
	processing map[uint32]struct{}
	doneMap    map[uint32]struct{}
}

func (r *nodeHelper) Init() {
	if r.pending == nil {
		r.pending = make(map[uint32]struct{})
	}
	if r.processing == nil {
		r.processing = make(map[uint32]struct{})
	}
	if r.doneMap == nil {
		r.doneMap = make(map[uint32]struct{})
	}

}
func (r *nodeHelper) PushPending(id uint32) {
	r.pending[id] = struct{}{}

}
func (r *nodeHelper) CheckPrepare() []uint32 {
	var ret []uint32
	for id := range r.pending {
		if ok := r.checkprocess(id); !ok {
			continue
		} else {
			delete(r.pending, id)
			r.processing[id] = struct{}{}
			ret = append(ret, id)
		}
	}
	return ret
}
func (r *nodeHelper) SetDone(id uint32, v any) {
	r.doneMap[id] = struct{}{}
	delete(r.processing, id)

}

func (r *nodeHelper) checkprocess(id uint32) bool {
	before := r.g.GetDepend(id)
	for _, preId := range before {
		_, ok := r.doneMap[preId]
		if !ok {
			return false
		}
	}
	value := make(map[string]any, len(before))
	for _, id := range before {
		nod := r.g.FindNode(id)
		value[nod.Name] = r.doneMap[nod.Id]
	}
	return true

}
