package expr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGroupID(t *testing.T) {
	for i := uint16(0); i < 128; i++ {
		gid := newGroupID(i, 0x0).Value()

		require.NotEmpty(t, gid[2:])
		require.Equal(t, i, gid.Size())

		// check unsafe size method works
		// gid := internedGID.Value()
		// require.EqualValues(t, int(i), int(internedGID.Size()))
	}
}
