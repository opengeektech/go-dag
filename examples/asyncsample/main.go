package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/opengeektech/go-dag/dag"
	"github.com/opengeektech/go-dag/graph"
)

func main() {
	displayTimeoutControl()

}

func displayTimeoutControl() {
	// key and value
	type Pair struct {
		Value int
		Node  string
	}
	g := dag.New[int, Pair]()
	g.SetName("test~")
	g.SetFunc("start", func(ctx context.Context, state *dag.State[int, Pair]) (Pair, error) {
		log.Printf("~step %s", state.CurrentNode.Name)
		time.Sleep(time.Second * 2)
		return Pair{
			Value: state.Input + 1,
			Node:  "output from start",
		}, nil
	})
	g.SetFunc("A", func(ctx context.Context, state *dag.State[int, Pair]) (Pair, error) {
		log.Printf("~step %s", state.CurrentNode.Name)
		time.Sleep(time.Second*2)
		// return state.Last + 1, nil
		return Pair{
			Value: state.Last.Value + 1,
			Node:  "output from A",
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
	ctx, abort := context.WithTimeout(context.TODO(), time.Second*3)
	defer abort()
	// w, err := g.RunSync(context.TODO(), 1)
	w, err := g.RunAsync(ctx, 1)
	if err != nil {
		fmt.Println("output>  ",w, err)
		return
	}
	fmt.Println("output> ", w)
}
