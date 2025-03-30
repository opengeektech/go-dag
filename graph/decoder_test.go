package graph

import (
	"github.com/opengeektech/go-dag/utils"
	"os"
	"testing"
)

func TestJsonDecoder_Decode(t *testing.T) {
	t.Run("test~read_nodes", func(t *testing.T) {
		all, err := os.ReadFile("tests/nodes.json")
		if err != nil {
			t.Error(err)
			return
		}
		var h JsonDecoder 
		g,err := h.Decode((all),)
		if err != nil {
			t.Error(err)
			return
		}
		if g==nil {
			t.Fatal("nil graph")
			
		}
		it := g.TopoIterator()
		for it.HasNext() {
			b,err := it.Next()
			if err != nil {
				t.Error(err)
				return
			}
			t.Log(utils.JsonString(b))
		}
	})

}
