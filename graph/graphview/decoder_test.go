package graphview

import (
	"bytes"
	"os"
	"testing"

	"github.com/opengeektech/go-dag/utils"
)

func TestJsonDecoder_Decode(t *testing.T) {
	t.Run("test~read_nodes", func(t *testing.T) {
		all, err := os.ReadFile("tests/nodes.json")
		if err != nil {
			t.Error(err)
			return
		}
		var h JsonDecoder 
		g,err := h.Decode(bytes.NewBuffer(all))
		if err != nil {
			t.Error(err)
			return
		}
		if g==nil {
			t.Fatal("nil graph")
			
		}
		it := g[0].TopoIterator()
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
