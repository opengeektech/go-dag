package dag

import (
	"github.com/opengeektech/go-dag/graph"
	"context"
	"encoding/json"
	"sync"
	"testing"
)


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
	var g = ExecuteState[int,int] {
		Cond: sync.Cond{
			L: &sync.Mutex{},
		},
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
	// 1->  1+1=2  => 2 *2 = 4 => 4*2 =8
	last,err := g.RunAsync(context.TODO(),1)
	if err != nil {
		t.Error(err)
	}
	t.Log("last> ",last)
	last,err = g.RunSync(context.TODO(),1)
	if err != nil {
		t.Error(err)
	}
	t.Log("last> ",last)

}
func str(w any) string {
	bs,_ := json.Marshal(w)
	return string(bs)
}