package printer

import (
	"fmt"

	"github.com/logrusorgru/aurora"
)

func Error(str string) {
	fmt.Printf("%s: %s\n", aurora.Red(">> ERROR"), str)
}

func Print(str string) {
	fmt.Printf("%s\n", str)
}
