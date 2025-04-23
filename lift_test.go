package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLiftLiterals(t *testing.T) {
	tests := []struct {
		name         string
		expr         string
		expectedStr  string
		expectedArgs map[string]any
	}{
		{
			name:        "basic case",
			expr:        `event.name == "test/yolo"`,
			expectedStr: "event.name == vars.a",
			expectedArgs: map[string]any{
				"a": "test/yolo",
			},
		},
		{
			name:        "basic case with single quotes",
			expr:        `event.name == 'test/yolo'`,
			expectedStr: "event.name == vars.a",
			expectedArgs: map[string]any{
				"a": "test/yolo",
			},
		},
		{
			name:        "multiple values",
			expr:        `event.name == "test/yolo" || event.name == 'test/foobar'`,
			expectedStr: "event.name == vars.a || event.name == vars.b",
			expectedArgs: map[string]any{
				"a": "test/yolo",
				"b": "test/foobar",
			},
		},
		{
			name: "multiple lines with comments",
			expr: `
			// we're breaking it
			event.name == "test/yolo" || event.name == 'test/foobar'
			`,
			expectedStr: "event.name == vars.a || event.name == vars.b",
			expectedArgs: map[string]any{
				"a": "test/yolo",
				"b": "test/foobar",
			},
		},
		{
			name:         "division operator",
			expr:         `event.ts / 1000 > 1745436368`,
			expectedStr:  `event.ts / 1000 > 1745436368`,
			expectedArgs: map[string]any{},
		},
		{
			name:         "ignore comments",
			expr:         `// foo`,
			expectedStr:  "",
			expectedArgs: map[string]any{},
		},
		{
			name:        "ignore trailing comments",
			expr:        `event.name == "test/yolo" // foo`,
			expectedStr: "event.name == vars.a",
			expectedArgs: map[string]any{
				"a": "test/yolo",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expr, vars := liftLiterals(test.expr)

			assert.Equal(t, test.expectedStr, expr, test.expr)
			assert.Equal(t, test.expectedArgs, vars.Map())
		})
	}
}
