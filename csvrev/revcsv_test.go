package csvrev

import (
	"fmt"
	"strings"
)

func ExampleScanner() {
	input := strings.NewReader("Header\nLine1\nLine2\nLine3")
	inputLen := input.Len()

	scanner := NewOptions(input, inputLen, &Options{Header: true})
	for {
		line, pos, err := scanner.Line()
		if err != nil {
			fmt.Println("Error:", err)
			break
		}
		fmt.Printf("Line position: %2d, line: %q\n", pos, line)
	}

	// Output:
	// Line position:  0, line: "Header"
	// Line position: 19, line: "Line3"
	// Line position: 13, line: "Line2"
	// Line position:  7, line: "Line1"
	// Error: EOF
}
