package dag

import (
	"context"
	"github.com/opengeektech/go-dag/graph"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type ExecuteState[K, V any] struct {
	Cond  sync.Cond
	Funcs map[string]HandlerFunc[K, V]
	G     *graph.Graph
	It    *graph.TopoIterator
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

func (r *ExecuteState[K, V]) RunSync(ctx context.Context, input K) (V, error) {
	it := r.G.TopoIterator()
	r.It = it
	var last V
	var prevs = make(map[string]result[V])
	var ordId = uint32(1)
	for r.It.HasNext() {
		nodeList, err := r.It.Next()
		if err != nil {
			var e V
			return e, err
		}
		for _, currNode := range nodeList {
			if currNode == nil {
				continue
			}
			handler, ok := r.Funcs[currNode.Name]
			if !ok || handler == nil {
				continue
			}
			var lastoutput = make(map[string]V)
			dep := r.G.GetDepend(currNode.Id)
			var nodeOrder = make(map[string]uint32, len(dep))
			for _, v := range dep {
				depNode := r.G.FindNode(v)
				val1 := prevs[depNode.Name]
				lastoutput[depNode.Name] = val1.V
				nodeOrder[depNode.Name] = val1.Order
			}

			o1, err := handler(ctx, &State[K, V]{
				Input:            input,
				DependNodeResult: lastoutput,
				NodeOrders:       nodeOrder,
				Last:             last,
				CurrentNode: *currNode,
			})
			prevs[currNode.Name] = result[V]{
				V:     o1,
				Node:  currNode,
				Err:   err,
				Order: ordId,
			}
			ordId++
			last = o1

			if err != nil {
				return last, err
			}
		}
	}

	return last, nil
}

type result[V any] struct {
	V     V
	Node  *graph.Node
	Err   error
	Order uint32
}

func (r *ExecuteState[K, V]) RunAsync(ctx context.Context, input K) (V, error) {
	it := r.G.TopoIterator()
	r.It = it
	var depstack dependstack
	var nodeResult = sync.Map{}
	ch := make(chan result[V], 1)
	// var last = atomic.Value{}
	var wg sync.WaitGroup
	var consumerWait = make(chan struct{})
	ctx, abort := context.WithCancel(ctx)
	defer abort()
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Printf("error panic  ON %v", e)
			}
			close(consumerWait)
		}()
	Outer:
		for {
			select {
			case val, opend := <-ch:
				r.Cond.L.Lock()
				if opend {
					node := val.Node
					ord := depstack.SetLast(node.Name, val)
					val.Order = ord
					nodeResult.Store(node.Name, val)
					// last.Store(val)
				}
				r.Cond.L.Unlock()
				r.Cond.Broadcast()
				if !opend {
					break Outer
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for r.It.HasNext() {
		nodeList, err := r.It.Next()
		if err != nil {
			var e V
			return e, err
		}
		for _, node := range nodeList {
			if node == nil {
				continue
			}
			//  stop for dispatch Next Node
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					err := recover()
					if err != nil {
						log.Printf("error panic ~ %v %s", err, debug.Stack())
					}
				}()
				if ctx.Err() != nil {
					return
				}

				var prevs map[string]V
				Skip := false
				var lastEle V
				var lastOrd uint32
				depMap := make(map[string]uint32)
				for !Skip {
					r.Cond.L.Lock()
					prevs = make(map[string]V)
					prevNodeId := r.G.GetDepend(node.Id)
					for _, v := range prevNodeId {
						nodex := r.G.FindNode(v)
						val, ok := nodeResult.Load(nodex.Name)
						// val, ok := nodeResult[nodex.Name]
						if !ok || val == nil {
							break
						}
						resultHolder, _ := val.(result[V])
						prevs[nodex.Name] = resultHolder.V
						if lastOrd <= resultHolder.Order {
							lastEle = resultHolder.V
							lastOrd = resultHolder.Order
						}
						depMap[nodex.Name] = resultHolder.Order
					}
					if len(prevNodeId) > 0 && len(prevNodeId) != len(prevs) {
						r.Cond.Wait()
					} else {
						Skip = true
					}

					r.Cond.L.Unlock()
				}
				if ctx.Err() != nil {
					return
				}
				handler, ok := r.Funcs[node.Name]
				if !ok || handler == nil {
					var e V
					ch <- result[V]{
						V:    e,
						Node: node,
					}
					return
				}

				o1, err := handler(ctx, &State[K, V]{
					Input:            input,
					DependNodeResult: prevs,
					Last:             lastEle,
					NodeOrders:       depMap,
					CurrentNode: *node,
				})
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					ch <- result[V]{
						V:    o1,
						Node: node,
						Err:  err,
					}
					return
				}

				ch <- result[V]{
					V:    o1,
					Node: node,
					Err:  err,
				}
			}()
		}
	}
	wg.Wait()
	close(ch)
	<-consumerWait
	var e V

	h := depstack.last.Load()
	if h == nil {
		return e, nil
	}
	val, _ := h.(result[V])
	return val.V, nil
}
