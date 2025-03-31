package dag

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/opengeektech/go-dag/graph/graphview"
)

func _loadDag(print bool) (*Dag[int, int], error) {
	var h graphview.JsonDecoder
	f, err := os.Open("../graph/graphview/tests/nodes.json")
	if err != nil {
		return nil, err
	}
	graphlist, err := h.Decode(f)
	if err != nil {
		return nil, err
	}
	d := New[int, int]()
	d.SetGraph(graphlist[0])
	// register func for by Node Name
	d.SetFunc("Default", func(c context.Context, st *State[int, int]) (int, error) {
		return 0, nil
	})
	// register func for any Node
	d.RegisterFunc(func(s string, k uint32) HandlerFunc[int, int] {
		return func(c context.Context, st *State[int, int]) (int, error) {
			o := 0
			if st.Last == 0 {
				o = st.Input + 1
			} else {
				o = st.Last + 1
			}
			if print {

				fmt.Printf("Node %s -> %d \n", s, o)
			}
			return o, nil
		}
	})
	return d, nil
}
func TestRun_dag(t *testing.T) {
	w, err := _loadDag(true)
	if err != nil {
		t.Fatal(err)
	}
	SetGlobalDag(w.Name(),w)
	t.Run("case-1",func (t *testing.T) {
		if err != nil {
			t.Error(err)
			return
		}
		k, err := w.RunAsync(context.TODO(), 0)
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("output ret > ", k)
	})
	t.Run("case-2",func (t *testing.T) {
		k, err := RunAsync[int,int](w.Name(),context.TODO(), 0)
		if err != nil {
			t.Error(err)
			return
		}
		t.Log("output ret > ", k)
	})
}
	

func Benchmark_Run_dag(t *testing.B) {

	var err error
	w, err := _loadDag(false)
	if err != nil {
		t.Error(err)
		return
	}
	var k int
	for i := 0; i < t.N*10000; i++ {
		k, err = w.RunAsync(context.TODO(), 0)
		if err != nil {
			t.Error(err)
			return
		}
		if k != 4 {
			t.Fatal("NOT OK ",k)
		}
	}

	// t.Log("output ret > ", k)

}
