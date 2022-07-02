package wasm

import (
	"context"
	"fmt"
	"os"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/bytecodealliance/wasmtime-go"
)

func processorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Utility").
		Summary("TODO").
		Field(service.NewStringField("path").Description("The path of the target WASM module to execute.")).
		Field(service.NewStringField("function_name").Description("The name of an exported function to run for each message.").Default("process_message")).
		Version("4.3.0")
}

func init() {
	err := service.RegisterProcessor(
		"wasm", processorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newProcessorFromConfig(conf)
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type processor struct {
	store *wasmtime.Store
	fn    *wasmtime.Func
}

func newProcessorFromConfig(conf *service.ParsedConfig) (*processor, error) {
	pathStr, err := conf.FieldString("path")
	if err != nil {
		return nil, err
	}

	fileBytes, err := os.ReadFile(pathStr)
	if err != nil {
		return nil, err
	}

	funcName, err := conf.FieldString("function_name")
	if err != nil {
		return nil, err
	}

	return newProcessor(funcName, fileBytes)
}

func newProcessor(funcName string, wasmBinary []byte) (*processor, error) {
	store := wasmtime.NewStore(wasmtime.NewEngine())
	module, err := wasmtime.NewModule(store.Engine, wasmBinary)
	if err != nil {
		return nil, err
	}

	instance, err := wasmtime.NewInstance(store, module, []wasmtime.AsExtern{})
	if err != nil {
		return nil, err
	}

	fn := instance.GetFunc(store, funcName)
	if fn == nil {
		return nil, fmt.Errorf("function %v missing from module", funcName)
	}

	return &processor{
		store: store,
		fn:    fn,
	}, nil
}

func (p *processor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	inBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	res, err := p.fn.Call(p.store, inBytes)
	if err != nil {
		return nil, err
	}

	newMsg := msg.Copy()
	newMsg.SetStructured(res)
	return service.MessageBatch{newMsg}, nil
}

func (p *processor) Close(ctx context.Context) error {
	return nil
}
