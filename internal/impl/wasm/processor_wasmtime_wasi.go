package wasm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bytecodealliance/wasmtime-go"

	"github.com/benthosdev/benthos/v4/public/service"
)

func wasmtimeWASIProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Utility").
		Summary("Executes a WASI-compliant WASM module.").
		Description(`
For each message the serialised contents are fed into the WASM module as an input argument. Once the module exits the results written to stdout will replace the message contents.

By default Benthos does not build with components that require linking to external libraries. If you wish to build Benthos locally with this component then set the build tag ` + "`x_benthos_extra`" + `:

` + "```shell" + `
# With go
go install -tags "x_benthos_extra" github.com/benthosdev/benthos/v4/cmd/benthos@latest

# Using make
make TAGS=x_benthos_extra
` + "```" + `

There is a specific docker tag postfix ` + "`-cgo`" + ` for C builds containing this component.`).
		Field(service.NewStringField("path").
			Description("The path of the target WASI module to execute.")).
		Field(service.NewStringField("io_dir").
			Description("An optional directory to store stdout temporary files. When omitted a temporary directory is created.").
			Optional()).
		Version("X.X.X")
}

// func init() {
// 	err := service.RegisterProcessor(
// 		"wasm_wasi", wasmtimeWASIProcessorConfig(),
// 		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
// 			return newWasmtimeWASIProcessorFromConfig(conf)
// 		})

// 	if err != nil {
// 		panic(err)
// 	}
// }

//------------------------------------------------------------------------------

type wasmtimeWASIProcessor struct {
	engine *wasmtime.Engine
	module *wasmtime.Module
	linker *wasmtime.Linker

	stdoutPath  string
	ioDir       string
	removeIODir bool
}

func newWasmtimeWASIProcessorFromConfig(conf *service.ParsedConfig) (*wasmtimeWASIProcessor, error) {
	pathStr, err := conf.FieldString("path")
	if err != nil {
		return nil, err
	}

	fileBytes, err := os.ReadFile(pathStr)
	if err != nil {
		return nil, err
	}

	var ioDir string
	if conf.Contains("io_dir") {
		if ioDir, err = conf.FieldString("io_dir"); err != nil {
			return nil, err
		}
	}

	return newWasmtimeWASIProcessor(fileBytes, ioDir)
}

func newWasmtimeWASIProcessor(wasmBinary []byte, ioDir string) (*wasmtimeWASIProcessor, error) {
	engine := wasmtime.NewEngine()

	module, err := wasmtime.NewModule(engine, wasmBinary)
	if err != nil {
		return nil, err
	}

	linker := wasmtime.NewLinker(engine)
	if err := linker.DefineWasi(); err != nil {
		return nil, err
	}

	p := &wasmtimeWASIProcessor{
		engine: engine,
		module: module,
		linker: linker,
	}

	if ioDir == "" {
		if ioDir, err = os.MkdirTemp("", "benthos_wasi_proc"); err != nil {
			return nil, err
		}
		p.removeIODir = true
	}
	p.ioDir = ioDir
	p.stdoutPath = filepath.Join(p.ioDir, "tmp.stdout")

	return p, nil
}

func (p *wasmtimeWASIProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	inBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	if err := os.Truncate(p.stdoutPath, 0); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	wasiConfig := wasmtime.NewWasiConfig()
	wasiConfig.SetArgv([]string{"BENTHOS_WASI", string(inBytes)})
	if err := wasiConfig.SetStdoutFile(p.stdoutPath); err != nil {
		return nil, err
	}

	store := wasmtime.NewStore(p.engine)
	store.SetWasi(wasiConfig)

	instance, err := p.linker.Instantiate(store, p.module)
	if err != nil {
		return nil, fmt.Errorf("failed to create wasmtime instance: %w", err)
	}

	nom := instance.GetFunc(store, "_start")
	if _, err := nom.Call(store); err != nil {
		return nil, fmt.Errorf("failed to call _start: %w", err)
	}

	out, err := os.ReadFile(p.stdoutPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read stdout: %w", err)
	}

	msg.SetBytes(out)
	return service.MessageBatch{msg}, nil
}

func (p *wasmtimeWASIProcessor) Close(ctx context.Context) error {
	if p.removeIODir {
		return os.RemoveAll(p.ioDir)
	}
	_ = os.Remove(p.stdoutPath)
	return nil
}
