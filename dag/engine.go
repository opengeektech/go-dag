package dag

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/opengeektech/go-dag/graph"
)

type ExecuteState[K, V any] struct {
	protect   sync.RWMutex
	Funcs     map[string]HandlerFunc[K, V]
	G         *graph.Graph
	GraphIter *graph.TopoIterator

	NodeResult map[uint32]any
	NodeOrder  map[uint32]uint32
	Active     chan uint32
	Recv       chan uint32
	OrdIdAlloc uint32
}


func (r *ExecuteState[K, V]) sendChan(ch chan uint32, k uint32) {
	defer func() {
		f := recover()
		if f != nil {
			log.Printf(" error Send Chan: %v", f)
		}
	}()
	ch <- k
}
func (r *ExecuteState[K, V]) write(fn func()) {
	r.protect.Lock()
	defer r.protect.Unlock()
	fn()
}
func (r *ExecuteState[K, V]) read(fn func()) {
	r.protect.RLock()
	defer r.protect.RUnlock()
	fn()
}

type HandlerFunc[Input any, Output any] func(ctx context.Context, s *State[Input, Output]) (Output, error)

type State[K any, V any] struct {
	Input            K
	Last             V
	CurrentNode      graph.Node
	NodeOrders       map[string]uint32
	DependNodeResult map[string]V
}

type dependstack struct {
	// nodeResult sync.Map
	last  atomic.Value
	Order uint32
}

func (r *dependstack) SetLast(name string, val any) uint32 {
	nv := atomic.AddUint32(&r.Order, 1)
	r.last.Store(val)
	return nv
}
func (r *ExecuteState[K, V]) iterclose() {
	fn := func(ch chan uint32) {
		defer func() {
			err := recover()
			if err != nil {
				log.Printf("close chan error %+v", err)
			}

		}()
		close(ch)
	}
	// fn(r.Active)
	fn(r.Recv)
}
func (r *ExecuteState[K, V]) ensure() {
	if r.NodeOrder == nil {
		r.NodeOrder = make(map[uint32]uint32)
	}
	if r.NodeResult == nil {
		r.NodeResult = make(map[uint32]any)
	}
}
func (r *ExecuteState[K, V]) RunAsync(ctx context.Context, input K) (V, error) {
	r.ensure()
	var (
		v   V
		err error
	)
	defer r.iterclose()
	type tmp struct {
		V   V
		Err error
	}
	ch := r.IterChan()
	out := make(chan tmp, 1)
	defer close(out)
	for {
		if ctx.Err() != nil {
			return v, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return v,ctx.Err()
		case nodeId, ok := <-ch:
			if !ok {
				return v, err
			}
			if nodeId == 0 {
				panic("illgal NodeId ")
			}
			go func() {
				gg := r.G.FindNode(nodeId)
				t1, err := r.RunNodeBlock(ctx, gg, input)
				if ctx.Err() != nil {
					return
				}
				out <- tmp{
					V:   t1,
					Err: err,
				}
			}()
		case out, ok := <-out:
			if !ok {
				return v, err
			}
			if out.Err != nil {
				return out.V, out.Err
			}
			v = out.V
			err = out.Err
		}
	}
}

func (r *ExecuteState[K, V]) RunSync(ctx context.Context, input K) (V, error) {
	r.ensure()
	var (
		v   V
		err error
	)
	defer r.iterclose()
	for nodeId := range r.Iter() {
		if nodeId == 0 {
			panic("illgal NodeId ")
		}
		if ctx.Err() != nil {
			return v, ctx.Err()
		}
		v, err = r.RunNodeBlock(ctx, r.G.FindNode(nodeId), input)
		if err != nil {
			return v, err
		}
	}
	return v, err
}

type NodeOutput[V any] struct {
	V     V
	Node  *graph.Node
	Err   error
	Order uint32
	Valid bool
}

func (r *ExecuteState[K, V]) iterDone(id uint32) {
	r.sendChan(r.Recv, id)
}
func (r *ExecuteState[K, V]) IterChan() chan uint32 {
	if r.GraphIter != nil {
		panic("graph iterator is not nil")
	}
	it := r.G.TopoIterator()
	r.GraphIter = it
	r.Active = make(chan uint32, 1)
	r.Recv = make(chan uint32, 1)
	go func() {
		r.doPush(r.Active, r.Recv)
	}()
	return r.Active
}

func (state *ExecuteState[K, V]) RunNodeBlock(ctx context.Context, node *graph.Node, input K) (V, error) {
	depend := state.G.GetDepend(node.Id)
	var lastNodeOutput V
	var ordId = uint32(0)
	var resultMap = make(map[string]V)
	var ordMap = make(map[string]uint32)
	state.read(func() {
		for _, depId := range depend {
			ord, ok := state.NodeOrder[depId]
			if !ok {
				panic(fmt.Sprintf("node %d not found", depId))
			}
			dependResult, _ := state.NodeResult[depId].(*NodeOutput[V])
			if ord >= ordId {
				lastNodeOutput = dependResult.V
				ordId = dependResult.Order
			}
			if dependResult != nil {
				resultMap[dependResult.Node.Name] = dependResult.V
				ordMap[dependResult.Node.Name] = dependResult.Order
			}
		}
	})
	st := &State[K, V]{
		Input:            input,
		DependNodeResult: resultMap,
		NodeOrders:       ordMap,
		CurrentNode:      *node,
		Last:             lastNodeOutput,
	}
	handler, ok := state.Funcs[node.Name]
	if !ok || handler == nil {
		state.write(func() {
			state.OrdIdAlloc++
			state.NodeOrder[node.Id] = state.OrdIdAlloc
			state.NodeResult[node.Id] = &NodeOutput[V]{
				V:     lastNodeOutput,
				Node:  node,
				Err:   nil,
				Order: state.OrdIdAlloc,
				Valid: false,
			}
			state.sendChan(state.Recv, node.Id)
		})
		return lastNodeOutput, nil
	}
	output, err := handler(ctx, st)
	state.write(func() {
		state.OrdIdAlloc++
		state.NodeOrder[node.Id] = state.OrdIdAlloc
		state.NodeResult[node.Id] = &NodeOutput[V]{
			V:     output,
			Node:  node,
			Err:   err,
			Order: state.OrdIdAlloc,
			Valid: true,
		}
		state.sendChan(state.Recv, node.Id)
	})
	return output, err

}
func (r *ExecuteState[K, V]) Iter() func(func(activeId uint32) bool) {
	ch := r.IterChan()
	return func(yield func(activeId uint32) bool) {
		for {
			id, active := <-ch
			if !active {
				break
			} else {
				if !yield(id) {
					return
				}
			}
		}
	}
}
func (r *ExecuteState[K, V]) doPush(activeNodeId chan uint32, recv chan uint32) {
	defer func() {
		f := recover()
		if f != nil {
			log.Printf(" error: %v", f)
		}
		close(activeNodeId)
	}()
	var h nodeHelper
	h.g = r.G
	h.Init()
Outer:
	for r.GraphIter.HasNext() {
		pending, err := r.GraphIter.Next()
		if err != nil {
			return
		}
		for _, v := range pending {
			h.PushPending(v.Id)
		}
		next := h.CheckPrepare()
		cursor := 0
		for cursor < len(next) {
			select {
			case activeNodeId <- next[cursor]:
				cursor++
			case val, active := <-recv:
				if active {
					h.SetDone(val, val)
				} else {
					break Outer
				}
			}
		}
		val, active := <-recv
		if active {
			h.SetDone(val, val)
		} else {
			break Outer
		}
	}

}
