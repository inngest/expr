package expr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGroupID(t *testing.T) {
	for i := uint16(0); i < 128; i++ {
		gid := newGroupID(i)
		require.Equal(t, i, gid.Size())
		require.NotEmpty(t, gid[2:])
	}
}
