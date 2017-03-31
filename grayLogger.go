package goBotCommon

import (
	"gopkg.in/aphistic/golf.v0"
	"fmt"
)

var (
	Graylog *GraylogType
	appName string
)

func NewGrayLog() *GraylogType {
	Graylog = &GraylogType{}
	return Graylog
}

type GraylogType struct {
	Client *golf.Client
}

func (g *GraylogType) Init(host string, port int64, name string) (err error) {
	g.Client, _ = golf.NewClient()
	g.Client.Dial(fmt.Sprintf("udp://%s:%v", host, port))
	appName = name
	return
}

func (g *GraylogType) L() (*golf.Logger) {
	l, _ := g.Client.NewLogger()
	l.SetAttr("app", appName)
	return l
}

func (g *GraylogType) Close() {
	g.Client.Close()
}

