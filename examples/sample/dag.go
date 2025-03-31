package main

import (
	"context"
	"fmt"
	"os"

	daglib "github.com/opengeektech/go-dag/dag"
	"github.com/opengeektech/go-dag/graph/graphview"
)

func main() {
	var h graphview.JsonDecoder
	f, err := os.Open("graph/graphview/tests/nodes.json")
	if err != nil {
		fmt.Println(err)
		return
	}
	k, err := h.Decode(f)
	if err != nil {
		fmt.Println(err)
		return
	}
	dag := daglib.New[int, int]()
	dag.SetGraph(k[0])
	dag.RegisterFunc(func(nodeName string, id uint32) daglib.HandlerFunc[int, int] {
		return func(c context.Context, st *daglib.State[int, int]) (int, error) {
			if st.Last > 0 {
				return st.Last + 1, nil
			}
			return st.Input + 1, nil
		}
	})
	// case 1
	{
		w, err := dag.RunSync(context.Background(), 1)
		if err != nil {
			fmt.Println("err ", err)
			return
		}
		fmt.Println(w)
	}
	// case 2
	{

		daglib.SetGlobalDag(dag.Name(), dag)

		name := dag.Name()
		// run by name
		w, err := daglib.Run[int, int](name, context.Background(), 1)
		if err != nil {
			fmt.Println("err ", err)
			return
		}
		fmt.Println(w)
	}

}
