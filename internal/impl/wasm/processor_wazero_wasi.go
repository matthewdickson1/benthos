package wasm

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"

	"github.com/benthosdev/benthos/v4/public/service"
)

func wazeroWASIProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Utility").
		Summary("Executes a WASI-compliant WASM module.").
		Description(`
For each message the serialised contents are fed into the WASM module as an input argument. Once the module exits the results written to stdout will replace the message contents.`).
		Field(service.NewStringField("path").
			Description("The path of the target WASI module to execute.")).
		Version("X.X.X")
}

func init() {
	err := service.RegisterProcessor(
		"wasm_wasi", wazeroWASIProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newWazeroWASIProcessorFromConfig(conf)
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type wazeroWASIProcessor struct {
	runtime wazero.Runtime
	code    wazero.CompiledModule
}

func newWazeroWASIProcessorFromConfig(conf *service.ParsedConfig) (*wazeroWASIProcessor, error) {
	pathStr, err := conf.FieldString("path")
	if err != nil {
		return nil, err
	}

	fileBytes, err := os.ReadFile(pathStr)
	if err != nil {
		return nil, err
	}

	return newWazeroWASIProcessor(fileBytes)
}

func newWazeroWASIProcessor(wasmBinary []byte) (*wazeroWASIProcessor, error) {
	ctx := context.Background()

	r := wazero.NewRuntime(ctx)
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, r); err != nil {
		_ = r.Close(ctx)
		return nil, err
	}

	code, err := r.CompileModule(ctx, wasmBinary)
	if err != nil {
		_ = r.Close(ctx)
		return nil, err
	}

	return &wazeroWASIProcessor{
		runtime: r,
		code:    code,
	}, nil
}

func (p *wazeroWASIProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	inBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	var outBuf, errBuf bytes.Buffer
	config := wazero.NewModuleConfig().
		WithStdout(&outBuf).WithStderr(&errBuf)

	mod, err := p.runtime.InstantiateModule(ctx, p.code, config.WithArgs("benthos_wasi", string(inBytes)))
	_ = mod.Close(ctx)
	if err != nil {
		// Note: Most compilers do not exit the module after running "_start",
		// unless there was an error. This allows you to call exported functions.
		if exitErr, ok := err.(*sys.ExitError); ok && exitErr.ExitCode() != 0 {
			return nil, fmt.Errorf("exit_code: %d", exitErr.ExitCode())
		} else if !ok {
			return nil, err
		}
	}

	if errBuf.Len() > 0 {
		return nil, fmt.Errorf("wasi stderr: %s", errBuf.Bytes())
	}

	msg.SetBytes(outBuf.Bytes())
	return service.MessageBatch{msg}, nil
}

func (p *wazeroWASIProcessor) Close(ctx context.Context) error {
	return p.runtime.Close(ctx)
}
