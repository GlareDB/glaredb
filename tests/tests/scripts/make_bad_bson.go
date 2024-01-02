package main

import (
	"os"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/types"
	"github.com/tychoish/fun"
	"github.com/tychoish/fun/ft"
)

func main() {
	fn, err := os.Create("dupes.bson")
	fun.Invariant.Must(err)
	defer func() { fun.Invariant.Must(fn.Close()) }()

	for i := 0; i < 100; i++ {
		n := ft.Must(
			birch.DC.Make(3).Append(
				birch.EC.ObjectID("_id", types.NewObjectID()),
				birch.EC.Int32("idx", 1),
				birch.EC.String("dupe", "first"),
				birch.EC.String("dupe", "second"),
			).WriteTo(fn))
		fun.Invariant.IsTrue(n == 64, "unexpected document length", n)
	}
}
