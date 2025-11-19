package main

import (
	"fmt"
	"os"
)

func greet(name string) string {
	return fmt.Sprintf("Hello, %s!", name)
}

func main() {
	name := "World"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	fmt.Println(greet(name))
}
