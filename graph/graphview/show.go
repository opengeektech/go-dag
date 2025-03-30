package graphview

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/opengeektech/go-dag/graph"
)

func GetDotGraphDesc(g *graph.Graph) (string,error) {
	var buf strings.Builder
	buf.WriteString("digraph G {\n")
	arr := g.GetEdgeList()
	for _,v := range arr {
		f := g.FindNode(v[0])
		t := g.FindNode(v[1])
		buf.WriteString(" ")
		buf.WriteString(fmt.Sprintf("%s -> %s",f.Name,t.Name))
		buf.WriteString(";\n")

	}
	buf.WriteString("}")
	return buf.String(),nil
}
func GetDotGraphDescLink(g *graph.Graph) string {
	s,_ := GetDotGraphDesc(g)
	ns := url.PathEscape(s)
	s2 := `https://dreampuf.github.io/GraphvizOnline/?engine=dot#` + ns
	return s2
}