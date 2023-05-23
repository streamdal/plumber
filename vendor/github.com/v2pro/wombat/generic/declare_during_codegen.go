//+build codegen

package generic

var inDeclaringByExample = false

type funcDeclaration struct {
	funcTemplate *FuncTemplate
	templateArgs []interface{}
}

var funcDeclarations = []funcDeclaration{}

func Declare(example func()) {
	defer func() {
		inDeclaringByExample = false
	}()
	inDeclaringByExample = true
	example()
}
