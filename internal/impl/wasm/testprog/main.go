package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	for _, arg := range os.Args {
		fmt.Printf("%v WASM RULES\n", strings.ToUpper(arg))
	}
}
