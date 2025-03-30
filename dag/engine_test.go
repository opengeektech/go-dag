package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/opengeektech/go-dag/graph"
	"go.uber.org/goleak"
)
func TestMain(m *testing.M) {
	defer goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}


func Test_print(t *testing.T) {
	gg := graph.NewGraph(
		func(c *graph.Graph) {
			c.AddNode(&graph.Node{
				Name: "a",
			})
			c.AddNode(&graph.Node{
				Name: "b",
			})
			c.AddNode(&graph.Node{
				Name: "c",
			})
		
		},
	)
	gg.DependOn("b","a")
	gg.DependOn("c","b","a")
	var factory = func () *ExecuteState[int,int] {
		var g = &ExecuteState[int,int] {
			Funcs: map[string]HandlerFunc[int, int]{
				"a": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step1 ",str(s))
					return s.Input+1, nil
				},
				"b": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step2",str(s))
					return s.DependNodeResult["a"]*2, nil
				},
				"c": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step3",str(s))
					return s.DependNodeResult["b"]*2, nil
				},
			},
			G: gg,
		}
		return g
	}
	
	// 1->  1+1=2  => 2 *2 = 4 => 4*2 =8
	last,err := factory().RunAsync(context.TODO(),1)
	if err != nil {
		t.Error(err)
	}
	t.Log("last> ",last)
	last,err =  factory().RunSync(context.TODO(),1)
	if err != nil {
		t.Error(err)
	}
	t.Log("last> ",last)

}
func str(w any) string {
	bs,_ := json.Marshal(w)
	return string(bs)
}


func Test_iter(t *testing.T) {
	gg := graph.NewGraph(
		func(c *graph.Graph) {
			c.AddNode(&graph.Node{
				Name: "a",
			})
			c.AddNode(&graph.Node{
				Name: "b",
			})
			c.AddNode(&graph.Node{
				Name: "c",
			})
		
		},
	)
	gg.DependOn("b","a")
	gg.DependOn("c","b","a")
	var g = ExecuteState[int,int] {
		
		Funcs: map[string]HandlerFunc[int, int]{
			"a": func(ctx context.Context, s *State[int, int]) (int, error) {
				t.Log("step1 ",str(s))
				return s.Input+1, nil
			},
			"b": func(ctx context.Context, s *State[int, int]) (int, error) {
				t.Log("step2",str(s))
				return s.DependNodeResult["a"]*2, nil
			},
			"c": func(ctx context.Context, s *State[int, int]) (int, error) {
				t.Log("step3",str(s))
				return s.DependNodeResult["b"]*2, nil
			},
		},
		G: gg,
	}
	ch := g.IterChan()
	for {
		id,ok := <-ch
		if !ok {
			break
		}
		fmt.Println("iter ",id)
		g.iterDone(id)
	}
}

func Test_iter2(t *testing.T) {
	gg := graph.NewGraph(
		func(c *graph.Graph) {
			c.AddNode(&graph.Node{
				Name: "a",
			})
			c.AddNode(&graph.Node{
				Name: "b",
			})
			c.AddNode(&graph.Node{
				Name: "c",
			})
		
		},
	)
	gg.DependOn("b","a")
	gg.DependOn("c","b","a")
	t.Run("iter_graph",func (t *testing.T) {
		var g = ExecuteState[int,int] {
		
			Funcs: map[string]HandlerFunc[int, int]{
				"a": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step1 ",str(s))
					return s.Input+1, nil
				},
				"b": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step2",str(s))
					return s.DependNodeResult["a"]*2, nil
				},
				"c": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step3",str(s))
					return s.DependNodeResult["b"]*2, nil
				},
			},
			G: gg,
		}
		for NodeId := range g.Iter() {
			fmt.Println("iter ", NodeId)
			g.iterDone(NodeId)
		}
	})
	
	t.Run("iter_graph-2",func (t *testing.T) {
		var g = ExecuteState[int,int] {
		
			NodeResult: map[uint32]any{},
			NodeOrder:  map[uint32]uint32{},

			Funcs: map[string]HandlerFunc[int, int]{
				"a": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step1 ",str(s))
					return s.Input+1, nil
				},
				"b": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step2",str(s))
					return s.DependNodeResult["a"]*2, nil
				},
				"c": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step3",str(s))
					return s.DependNodeResult["b"]*2, nil
				},
			},
			G: gg,
		}
		for NodeId := range g.Iter() {
			fmt.Println("iter ", NodeId)
			// g.IterDone(NodeId)
			out,err := g.RunNodeBlock(context.TODO(), g.G.FindNode(NodeId), 1)
			if err != nil {
				t.Error(err)
			}
			t.Log("output > ",out)
		}
	})
	t.Run("iter_graph-3",func (t *testing.T) {
		var g = ExecuteState[int,int] {
	
			NodeResult: map[uint32]any{},
			NodeOrder:  map[uint32]uint32{},

			Funcs: map[string]HandlerFunc[int, int]{
				"a": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step1 ",str(s))
					return s.Input+1, nil
				},
				"b": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step2",str(s))
					return s.DependNodeResult["a"]*2, nil
				},
				"c": func(ctx context.Context, s *State[int, int]) (int, error) {
					t.Log("step3",str(s))
					return s.DependNodeResult["b"]*2, nil
				},
			},
			G: gg,
		}
		t.Log(g.RunSync(context.TODO(),1))
	})

}