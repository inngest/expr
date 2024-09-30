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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expr, vars := liftLiterals(test.expr)

			assert.Equal(t, test.expectedStr, expr)
			assert.Equal(t, test.expectedArgs, vars.Map())
		})
	}
}
