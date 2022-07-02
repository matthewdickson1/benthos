package wasm

import (
	"context"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/bytecodealliance/wasmtime-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWASMProcessor(t *testing.T) {
	wasm, err := wasmtime.Wat2Wasm(`TODO`)
	require.NoError(t, err)

	proc, err := newProcessor("foo", wasm)
	require.NoError(t, err)

	inMsg := service.NewMessage([]byte(`hello world`))
	outBatch, err := proc.Process(context.Background(), inMsg)
	require.NoError(t, err)

	require.Len(t, outBatch, 1)
	resBytes, err := outBatch[0].AsBytes()
	require.NoError(t, err)

	assert.Equal(t, "HELLO WORLD WASM RULES", string(resBytes))
}
