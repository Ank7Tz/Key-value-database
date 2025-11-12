package server

import "github.com/gin-gonic/gin"

type Server interface {
	Init()
	Start()
	Shutdown()
}

type RestServer struct {
	r *gin.Engine
}

func (server *RestServer) Init() {
	server.r = gin.Default()
}

func (server *RestServer) Start() {}

func (server *RestServer) Shutdown() {}
