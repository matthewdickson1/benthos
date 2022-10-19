package wasm

import (
	"context"
	"os"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWasmtimeWASIProcessor(t *testing.T) {
	wasm, err := os.ReadFile("./testprog/main.wasm")
	require.NoError(t, err)

	proc, err := newWasmtimeWASIProcessor(wasm, "")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, proc.Close(context.Background()))
	})

	inMsg := service.NewMessage([]byte(`hello world`))
	outBatch, err := proc.Process(context.Background(), inMsg)
	require.NoError(t, err)

	require.Len(t, outBatch, 1)
	resBytes, err := outBatch[0].AsBytes()
	require.NoError(t, err)

	assert.Equal(t, "BENTHOS_WASI WASM RULES\nHELLO WORLD WASM RULES\n", string(resBytes))
}

func BenchmarkWasmtimeWASICalls(b *testing.B) {
	wasm, err := os.ReadFile("./testprog/main.wasm")
	require.NoError(b, err)

	proc, err := newWasmtimeWASIProcessor(wasm, "")
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, proc.Close(context.Background()))
	})

	b.ResetTimer()
	b.ReportAllocs()

	inMsg := service.NewMessage([]byte(`hello world`))

	for i := 0; i < b.N; i++ {
		outBatch, err := proc.Process(context.Background(), inMsg.Copy())
		require.NoError(b, err)

		require.Len(b, outBatch, 1)

		_, err = outBatch[0].AsBytes()
		require.NoError(b, err)
	}
}
