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
			name:        "division operator",
			expr:        `event.ts / 1000 > 1745436368`,
			expectedStr: `event.ts / vars.a > vars.b`,
			expectedArgs: map[string]any{
				"a": int64(1000),
				"b": int64(1745436368),
			},
		},
		{
			name:         "hex literal not lifted",
			expr:         `event.data.flags == 0xFF`,
			expectedStr:  `event.data.flags == 0xFF`,
			expectedArgs: map[string]any{},
		},
		{
			name:        "leading-dot float with signed negative exponent",
			expr:        `event.data.x > .5e-2`,
			expectedStr: `event.data.x > vars.a`,
			expectedArgs: map[string]any{
				"a": float64(.5e-2),
			},
		},
		{
			name:        "leading-dot float with signed positive exponent",
			expr:        `event.data.x > .5e+3`,
			expectedStr: `event.data.x > vars.a`,
			expectedArgs: map[string]any{
				"a": float64(.5e+3),
			},
		},
		{
			name:        "scientific notation with explicit positive exponent",
			expr:        `event.data.count > 1e+6`,
			expectedStr: `event.data.count > vars.a`,
			expectedArgs: map[string]any{
				"a": float64(1e+6),
			},
		},
		{
			// cel-go parses .5 as a valid float literal (DIGIT* . DIGIT+, zero digits before dot).
			// Without special handling, the scanner writes "." then lifts "5" as int64 →
			// producing ".vars.a", which CEL rejects as a syntax error.
			name:        "leading-dot float lifted",
			expr:        `event.data.x > .5`,
			expectedStr: `event.data.x > vars.a`,
			expectedArgs: map[string]any{
				"a": float64(0.5),
			},
		},
		{
			name:        "leading-dot float with exponent lifted",
			expr:        `event.data.x > .5e2`,
			expectedStr: `event.data.x > vars.a`,
			expectedArgs: map[string]any{
				"a": float64(50),
			},
		},
		{
			// A dot between ident characters is a field-access separator, not a float.
			name:        "field access dot not confused with leading-dot float",
			expr:        `event.data.x == "ok"`,
			expectedStr: `event.data.x == vars.a`,
			expectedArgs: map[string]any{
				"a": "ok",
			},
		},
		{
			// u/U suffix makes it an unsigned integer literal in CEL.
			// Lifting just the digits would produce "vars.au" — a field access, not a uint.
			name:         "unsigned integer literal not lifted",
			expr:         `event.data.count == 42u`,
			expectedStr:  `event.data.count == 42u`,
			expectedArgs: map[string]any{},
		},
		{
			name:         "unsigned integer literal with capital U not lifted",
			expr:         `event.data.count == 100U`,
			expectedStr:  `event.data.count == 100U`,
			expectedArgs: map[string]any{},
		},
		{
			// Regular integer literals next to u/U that aren't a suffix (e.g. in a string)
			// should still be lifted normally.
			name:        "integer before unrelated u identifier",
			expr:        `event.data.id == 5 && event.data.unit == "kg"`,
			expectedStr: `event.data.id == vars.a && event.data.unit == vars.b`,
			expectedArgs: map[string]any{
				"a": int64(5),
				"b": "kg",
			},
		},
		{
			name:        "array index not lifted",
			expr:        `event.data.ids[1] == "id-b"`,
			expectedStr: `event.data.ids[1] == vars.a`,
			expectedArgs: map[string]any{
				"a": "id-b",
			},
		},
		{
			name:        "multi-dimensional array index not lifted",
			expr:        `event.data.ids[1] == "id-b" && event.data.ids[2] == "id-c"`,
			expectedStr: `event.data.ids[1] == vars.a && event.data.ids[2] == vars.b`,
			expectedArgs: map[string]any{
				"a": "id-b",
				"b": "id-c",
			},
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
