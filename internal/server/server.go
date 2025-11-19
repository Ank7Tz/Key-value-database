package server

import (
	"key_value_store/internal/raft"

	"github.com/gin-gonic/gin"
)

type CoreService struct {
	mr     raft.MultiRaft
	nodeID string
}

type RestServer struct {
	r    *gin.Engine
	cs   *CoreService
	addr string
}

func (server *RestServer) Init(cs *CoreService) {
	server.r = gin.Default()
	server.r.GET("/api/data/:key", server.ReadHandler)
	server.r.PUT("/api/data/:key", server.WriteHandler)
	server.r.DELETE("/api/data/:key", server.DeleteHandler)
	server.r.GET("/api/stats", server.StatsHandler)
}

func (server *RestServer) Run() error {
	return server.r.Run(server.addr)
}

func (server *RestServer) ReadHandler(ctx *gin.Context) {

}

func (server *RestServer) WriteHandler(ctx *gin.Context) {

}

func (server *RestServer) DeleteHandler(ctx *gin.Context) {

}

func (server *RestServer) StatsHandler(ctx *gin.Context) {

}
