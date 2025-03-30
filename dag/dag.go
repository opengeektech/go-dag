package dag

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/opengeektech/go-dag/graph"
)

type Dag[K, V any] struct {
	name    string
	graph   *graph.Graph
	funcMap atomic.Value
	valid bool
}
type Option[K, V any] func(d *Dag[K, V])

func WithName[K, V any](name string) Option[K, V] {
	return func(d *Dag[K, V]) {
		d.name = name
	}
}

func WithNodeFunc[K, V any](funcMapIn map[string]HandlerFunc[K, V]) Option[K, V] {
	return func(d *Dag[K, V]) {

		funcMap := make(map[string]HandlerFunc[K, V])
		w, _ := d.funcMap.Load().(map[string]HandlerFunc[K, V])
		for k, v := range w {
			funcMap[k] = v
		}
		for k, v := range funcMapIn {
			funcMap[k] = v
		}
		d.funcMap.Store(funcMap)
	}
}
func (d *Dag[K, V]) getFuncMap() map[string]HandlerFunc[K, V] {
	funcMap := make(map[string]HandlerFunc[K, V])
	w, _ := d.funcMap.Load().(map[string]HandlerFunc[K, V])
	for k, v := range w {
		funcMap[k] = v
	}
	return funcMap
}
func (r *Dag[K, V]) SetGraph(g *graph.Graph) *Dag[K, V] {
	r.graph = g
	return r
}
func (r *Dag[K, V]) SetName(n string) *Dag[K, V] {
	r.name = n
	return r
}
func (r *Dag[K, V]) Name() string {
	return r.name
}
func New[K, V any](opts ...Option[K, V]) *Dag[K, V] {
	return &Dag[K, V]{}
}

func (r *Dag[K, V]) HasFunc(nodeName string,) bool {
	funcMap := r.getFuncMap()
	if funcMap == nil {
		return false
	}
	_,ok := funcMap[nodeName]
	return ok
}
func (r *Dag[K, V]) SetFunc(nodeName string, fun HandlerFunc[K, V]) {
	
	funcMap := r.getFuncMap()
	newFunc := func(ctx context.Context, k *State[K, V]) (V, error) {
		return fun(ctx, k)
	}
	funcMap[nodeName] = newFunc
	r.funcMap.Store(funcMap)
}

func NewGraphDag[K any, V any](g *graph.Graph) *Dag[K, V] {
	return &Dag[K, V]{
		name:  g.GraphName,
		graph: g,
		valid: true,
	}
}
func (r *Dag[K, V]) RunAsync(ctx context.Context, k K) (V, error) {
	w := &ExecuteState[K, V]{
		Funcs: make(map[string]HandlerFunc[K, V]),
		G:     r.graph,
	}
	t := r.getFuncMap()
	for k, v := range t {
		w.Funcs[k] = v
	}
	defer func() {
		if w.GraphIter != nil {
			w.GraphIter.Reset()
			graph.GraphStatePool.Put(w.GraphIter)
		}
	}()
	v, err := w.RunAsync(ctx, k)
	return v, err
}
func (r *Dag[K, V]) RunSync(ctx context.Context, k K) (V, error) {
	w := &ExecuteState[K, V]{
		Funcs: make(map[string]HandlerFunc[K, V]),
		G:     r.graph,
	}
	t := r.getFuncMap()
	for k, v := range t {
		w.Funcs[k] = v
	}
	defer func() {
		if w.GraphIter != nil {
			w.GraphIter.Reset()
			graph.GraphStatePool.Put(w.GraphIter)
		}
	}()
	v, err := w.RunSync(ctx, k)
	return v, err
}

var (
	dagMap sync.Map
)

func SetGlobalDag[K, V any](Name string, d *Dag[K, V]) {
	if d == nil {
		dagMap.Delete(Name)
		return
	}
	dagMap.Store(Name, d)
}
func GetGlobalDag[K, V any](Name string) *Dag[K, V] {
	f, ok := dagMap.Load(Name)
	if !ok {
		return nil
	}
	h, _ := f.(*Dag[K, V])
	return h
}

func Run[K, V any](Name string, ctx context.Context, params K) (V, error) {
	dg := GetGlobalDag[K, V](Name)
	if dg == nil || !dg.valid {
		panic("dag load fail")
	}
	return dg.RunSync(ctx, params)
}
func RunConcurrent[K, V any](Name string, ctx context.Context, params K) (V, error) {
	dg := GetGlobalDag[K, V](Name)
	if dg == nil || !dg.valid {
		panic("dag load fail")
	}
	return dg.RunAsync(ctx, params)
}

func RunAsync[K, V any](Name string, ctx context.Context, params K) (V, error) {
	dg := GetGlobalDag[K, V](Name)
	if dg == nil || !dg.valid {
		panic("dag load fail")
	}
	return dg.RunAsync(ctx, params)
}
