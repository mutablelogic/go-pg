package pg

import (
	"testing"

	assert "github.com/stretchr/testify/assert"
)

func Test_OffsetLimit_Clamp_001(t *testing.T) {
	assert := assert.New(t)

	limit := uint64(50)
	offsetLimit := OffsetLimit{Offset: 0, Limit: &limit}
	offsetLimit.Clamp(100)

	assert.Equal(uint64(50), limit)
}

func Test_OffsetLimit_Clamp_002(t *testing.T) {
	assert := assert.New(t)

	limit := uint64(50)
	offsetLimit := OffsetLimit{Offset: 90, Limit: &limit}
	offsetLimit.Clamp(100)

	assert.Equal(uint64(10), limit)
}

func Test_OffsetLimit_Clamp_003(t *testing.T) {
	assert := assert.New(t)

	limit := uint64(50)
	offsetLimit := OffsetLimit{Offset: 100, Limit: &limit}
	offsetLimit.Clamp(100)

	assert.Equal(uint64(0), limit)
}

func Test_OffsetLimit_Clamp_004(t *testing.T) {
	assert := assert.New(t)

	limit := uint64(50)
	offsetLimit := OffsetLimit{Offset: 120, Limit: &limit}
	offsetLimit.Clamp(100)

	assert.Equal(uint64(0), limit)
}

func Test_OffsetLimit_Clamp_005(t *testing.T) {
	offsetLimit := OffsetLimit{Offset: 10, Limit: nil}
	offsetLimit.Clamp(100)
}
