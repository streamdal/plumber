package printer

import (
	"fmt"

	"github.com/logrusorgru/aurora"
)

// Error is a convenience function for printing errors.
func Error(str string) {
	fmt.Printf("%s: %s\n", aurora.Red(">> ERROR"), str)
}

// Print is a convenience function for printing regular output.
func Print(str string) {
	fmt.Printf("%s\n", str)
}

func PrintLogo() {
	logo := `
█▀█ █   █ █ █▀▄▀█ █▄▄ █▀▀ █▀█
█▀▀ █▄▄ █▄█ █ ▀ █ █▄█ ██▄ █▀▄`

	fmt.Println(logo)
}
