package main

import (
	"context"
	"fmt"
	"log"

	"github.com/opengeektech/go-dag/dag"
	"github.com/opengeektech/go-dag/graph"
)

func main() {
	// key and value
	type Pair struct {
		Value int
		Node  string
	}
	g := dag.New[int, Pair]()
	g.SetName("test~")
	g.SetFunc("start", func(ctx context.Context, state *dag.State[int, Pair]) (Pair, error) {
		log.Printf("~step %s", state.CurrentNode.Name)
		return Pair{
			Value: state.Input + 1,
			Node:  "start",
		}, nil
	})
	g.SetFunc("A", func(ctx context.Context, state *dag.State[int, Pair]) (Pair, error) {
		log.Printf("~step %s", state.CurrentNode.Name)
		// return state.Last + 1, nil
		return Pair{
			Value: state.Last.Value + 1,
			Node:  "A",
		}, nil
	})
	g.SetGraph(graph.NewGraph(graph.WithNodes(
		&graph.Node{Name: "start"},
		&graph.Node{Name: "A"},
		&graph.Node{Name: "B"},
		&graph.Node{Name: "C"},
	), graph.WithDependOn("A", "start"),
		graph.WithDependOn("B", "A"),
		graph.WithDependOn("C", "B"),
	))
	// w, err := g.RunSync(context.TODO(), 1)
	w, err := g.RunAsync(context.TODO(), 1)
	if err != nil {
		fmt.Println("err ", err)
	}
	fmt.Println("output> ", w)

}
