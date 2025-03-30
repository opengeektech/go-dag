package graphview

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/opengeektech/go-dag/graph"
)

type GraphContentHelper interface {
	Decode(s []byte) ([]*Graph, error)
}

type JsonDecoder struct {
}
type graphList struct {
	GraphList []graphJsonContent `json:"graphList"`
}
type graphJsonContent struct {
	GraphName string `json:"graphName"`
	GraphId   uint32 `json:"graphId"`
	Nodes     []*struct {
		Id           uint32   `json:"id"`
		Name         string   `json:"name"`
		DependOn     []uint32 `json:"dependOn"`
		DependOnName []string `json:"dependOnName"`
	} `json:"nodes"`
}

/*
{
"graphName":"graph",
"graphType": "dag",
"nodes": [

		[
			{
			"id": 1,
			"name": "node1",
			"dependOn": [2, 3]
			},
			{
			"id": 2,
			"name": "node2",
			"dependOn": [3]
			},
			{
			"id": 3,
			"name": "node3",
			"dependOn": []
		]
	}
*/
var (
	ErrIllegalContent = fmt.Errorf("Illegal Content check")
)
type Graph = graph.Graph
type Node = graph.Node
func (j *JsonDecoder) Decode(s []byte) ([]*Graph, error) {
	var h graphList
	err := json.Unmarshal(s, &h)
	if err != nil {
		return nil, err
	}
	var row []*Graph
	for _, v := range h.GraphList {
		g, err := j.decoderow(v)
		if err != nil {
			return row, err
		}
		row = append(row, g)

	}
	return row, nil
}

// Decode implements GraphContentHelper.
func (j *JsonDecoder) decoderow(t graphJsonContent) (*Graph, error) {

	g := graph.NewGraph()
	k := uint32(1)
	repeated := make(map[uint32]struct{})
	repeatedName := make(map[string]struct{})
	namebinding := make(map[string]*Node)
	nodeList := make(map[uint32]*Node)
	for _, v := range t.Nodes {
		if v.Id <= 0 {
			v.Id = k
			k++
		}
		if v.Name == "" {
			v.Name = "Node:" + strconv.Itoa(int(v.Id))
		}
		_, ok := repeated[v.Id]
		if ok {
			return nil, fmt.Errorf("%w,id config illegal %s", ErrIllegalContent, v.Name)
		}
		repeated[v.Id] = struct{}{}
		_, ok = repeatedName[v.Name]
		if ok {
			return nil, fmt.Errorf("%w,id config illegal %s", ErrIllegalContent, v.Name)
		}
		repeatedName[v.Name] = struct{}{}
		if len(v.DependOn) > 0 && len(v.DependOnName) > 0 {
			return nil, fmt.Errorf("%w dependOn and dependOnName is conflict", ErrIllegalContent)
		}
		t := &Node{
			Id:   v.Id,
			Name: v.Name,
		}
		namebinding[v.Name] = t
		nodeList[v.Id] = t
	}
	for _, ele := range t.Nodes {
		if len(ele.DependOnName) > 0 && len(ele.DependOn) == 0 {
			for _, v := range ele.DependOnName {
				if n, ok := namebinding[v]; !ok {
					return nil, fmt.Errorf("%w,dependOnName config illegal %s", ErrIllegalContent, v)
				} else {
					ele.DependOn = append(ele.DependOn, n.Id)
				}
			}
		}
	}

	for _, v := range t.Nodes {
		curr := nodeList[v.Id]
		if curr == nil {
			return nil, fmt.Errorf("Id error %w", ErrIllegalContent)
		}
		if len(v.DependOn) == 0 {
			g.DependOnNode(curr)
		}
		var pre []*Node
		for _, id := range v.DependOn {
			nod, ok := nodeList[id]
			if !ok {
				return nil, fmt.Errorf("node not found %w", ErrIllegalContent)
			}
			// g.dependOn(curr,nod)
			pre = append(pre, nod)
			g.DependOnNode(curr, pre...)
		}
	}
	return g, nil

}

var (
	_ GraphContentHelper = &JsonDecoder{}
)
