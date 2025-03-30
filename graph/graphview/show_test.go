package graphview

import (
	"os"
	"testing"

)

func TestShowDotGraph(t *testing.T) {
	all, err := os.ReadFile("tests/nodes.json")
	if err != nil {
		t.Error(err)
		return
	}
	var h JsonDecoder 
	g,err := h.Decode((all),)
	if err != nil {
		t.Fatal(err)
		return
	}
	s,err := GetDotGraphDesc(g[0])
	if err != nil {
		t.Error(err)
	}
	t.Log("\n"+s)
	t.Log("\n"+GetDotGraphDescLink(g[0]))
}
