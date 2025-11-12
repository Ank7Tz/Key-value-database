package main

import (
	"key_value_store/server"
)

func main() {
	var restServer server.Server = &server.RestServer{}
	restServer.Init()
	defer restServer.Shutdown()
	restServer.Start()
}
