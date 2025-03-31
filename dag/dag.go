package dag

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"maps"

	"github.com/opengeektech/go-dag/graph"
)

type Dag[K, V any] struct {
	name    string
	graph   *graph.Graph
	funcMap atomic.Value
	// valid   bool
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
	w, _ := d.funcMap.Load().(map[string]HandlerFunc[K, V])
	return w
}
func (d *Dag[K, V]) newFuncMap() map[string]HandlerFunc[K, V] {
	funcMap := make(map[string]HandlerFunc[K, V])
	w, _ := d.funcMap.Load().(map[string]HandlerFunc[K, V])
	for k, v := range w {
		funcMap[k] = v
	}
	return funcMap
}
func (r *Dag[K, V]) SetGraph(g *graph.Graph) *Dag[K, V] {
	r.graph = g
	r.name = g.GraphName
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

func (r *Dag[K, V]) HasFunc(nodeName string) bool {
	funcMap := r.newFuncMap()
	if funcMap == nil {
		return false
	}
	_, ok := funcMap[nodeName]
	return ok
}
func (r *Dag[K, V]) Wrapfunc(fun func(nodeName string, id uint32, fn HandlerFunc[K, V]) HandlerFunc[K, V]) {
	for _, node := range r.graph.NodeIdMapping {
		h := r.getFuncMap()

		if h != nil {
			w := h[node.Name]
			if w != nil {
				w = fun(node.Name, node.Id, w)
				r.SetFunc(node.Name, w)
			}

		}
	}
}
func (r *Dag[K, V]) RegisterFunc(fn func(nodeName string, id uint32) HandlerFunc[K, V]) {
	for _, node := range r.graph.NodeIdMapping {
		h := r.getFuncMap()
		if h == nil {
			r.SetFunc(node.Name, fn(node.Name, node.Id))
		} else if _, ok := h[node.Name]; !ok {
			r.SetFunc(node.Name, fn(node.Name, node.Id))
		}
	}
}
func (r *Dag[K, V]) SetFunc(nodeName string, fun HandlerFunc[K, V]) {

	funcMap := r.newFuncMap()
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
	}
}
func (r *Dag[K, V]) RunAsync(ctx context.Context, k K) (V, error) {
	w := &ExecuteState[K, V]{
		Funcs: make(map[string]HandlerFunc[K, V]),
		G:     r.graph,
	}
	t := r.newFuncMap()
	maps.Copy(w.Funcs, t)
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
	t := r.newFuncMap()
	maps.Copy(w.Funcs, t)
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
var (
	ErrDagNotFound = fmt.Errorf("Err dag Not found")
)

func Run[K, V any](Name string, ctx context.Context, params K) (V, error) {
	dg := GetGlobalDag[K, V](Name)
	if dg == nil || dg.graph == nil {
		var e V
		return e, ErrDagNotFound
	}
	return dg.RunSync(ctx, params)
}

func RunAsync[K, V any](Name string, ctx context.Context, params K) (V, error) {
	dg := GetGlobalDag[K, V](Name)
	if dg == nil || dg.graph == nil {
		var e V
		return e, ErrDagNotFound
	}
	return dg.RunAsync(ctx, params)
}
