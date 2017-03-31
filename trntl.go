package goBotCommon

import (
	"github.com/tarantool/go-tarantool"
	"fmt"
	"encoding/gob"
	"github.com/pkg/errors"
	"bytes"
	"github.com/tidwall/gjson"
)

var (
	Trntl *TrntlType
)

func NewTrnrl()  *TrntlType {
	Trntl = &TrntlType{}
	return Trntl
}

type TrntlType struct {
	Conn *tarantool.Connection
}

func (d *TrntlType) Init(user, password, host string, port int64) (err error) {
	//fmt.Printf("Config.Tarantool host: %s port: %s\n", Config.Tarantool.Host, Config.Tarantool.Port)
	opts := tarantool.Opts{User: user, Pass: password}
	connStr := fmt.Sprintf("%s:%v", host, port)
	if d == nil {
		d = &TrntlType{}
	}
	d.Conn, err = tarantool.Connect(connStr, opts)
	return
}

func (d *TrntlType) Close() {
	d.Conn.Close()
}

func (d *TrntlType) GetStructById(spaceName string, indexName string, key interface{}) (dec *gob.Decoder, err error) {
	resp, err := d.Conn.Select(spaceName, indexName, 0, 1, tarantool.IterEq, []interface{}{key})
	if err != nil {
		return
	}

	if len(resp.Data) == 0 {
		err = errors.New("value not found")
		return
	}

	tuple := resp.Data[0].([]interface{})

	if len(tuple) == 0 {
		err = errors.New("tuple not found")
		return
	}

	byteRes, ok := tuple[1].([]uint8); if !ok {
		err = errors.New("Can't convert tuple data to []uint8")
		return
	}

	dec = gob.NewDecoder(bytes.NewReader([]byte(byteRes)))
	return
}

func (d *TrntlType) Upsert(spaceName string, tuple, ops interface{}) (resp *tarantool.Response, err error) {
	resp, err = d.Conn.Ping()
	// TODO: возможно нужно вставить проверку коннекта с базой

	return d.Conn.Upsert(spaceName, tuple, ops)
}

func (d *TrntlType) Delete(spaceName, indexName string, ops interface{}) (resp *tarantool.Response, err error) {
	return d.Conn.Delete(spaceName, indexName, ops)
}

func (d *TrntlType) Call(functionName string, args interface{}) (resp *tarantool.Response, err error) {
	resp, err = d.Conn.Call17(functionName, args)
	if err != nil {
		err = fmt.Errorf("call '%s': %s", functionName, err)
	}
	return
}

func (d *TrntlType) Eval(expr string, args interface{}) (*tarantool.Response, error) {
	return d.Conn.Eval(expr, args)
}

func (d *TrntlType) SelectById(spaceName string, index string, id interface{}) (resp *tarantool.Response, err error) {
	resp, err = d.Conn.Select(spaceName, index, 0, 1, tarantool.IterEq, []interface{}{id})
	if err != nil {
		return
	}
	if len(resp.Data) == 0 {
		err = errors.New("value not found")
	}
	return
}

func (d *TrntlType) CallDbFunction(functionName string, args []interface{}) (result []byte, err error) {
	resp, err := d.Call(functionName, args)
	if err != nil {
		return
	}

	if len(resp.Data) == 0 {
		result = []byte{}
		return
	}

	queryRes := resp.Data[0].(string)

	//fmt.Printf("rsp %s\n", resp)
	code := gjson.Get(queryRes, "ok")
	if !code.Bool() {
		msg := gjson.Get(queryRes, "message").Str
		err = fmt.Errorf(msg)
	}
	result = []byte(gjson.Get(queryRes, "result").Raw)

	return
}
