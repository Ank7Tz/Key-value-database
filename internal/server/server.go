package server

import (
	"fmt"
	"key_value_store/internal/raft"
	"net/http"

	"github.com/gin-gonic/gin"
)

type WriteRequest struct {
	Value string `json:"value" binding:"required"`
}

type ReadResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
}

type WriteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type DeleteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type RestServer struct {
	engine *gin.Engine
	mr     *raft.MultiRaft
}

func (server *RestServer) handleRead(ctx *gin.Context) {
	key := ctx.Param("key")

	value, err := server.mr.Read(key)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, ReadResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, ReadResponse{
		Success: true,
		Value:   string(value),
	})
}

func (server *RestServer) handleWrite(ctx *gin.Context) {
	key := ctx.Param("key")

	var req WriteRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, WriteResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid request body: %v", err),
		})
		return
	}

	err := server.mr.Write(key, []byte(req.Value))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, WriteResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, WriteResponse{
		Success: true,
	})
}

func (server *RestServer) handleDelete(ctx *gin.Context) {
	key := ctx.Params.ByName("key")

	err := server.mr.Delete(key)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, DeleteResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, DeleteResponse{
		Success: true,
	})
}

func (server *RestServer) getStats(ctx *gin.Context) {
	response := server.mr.Stats()
	ctx.JSON(http.StatusOK, response)
}

func NewRestServer(mr *raft.MultiRaft) *RestServer {
	server := &RestServer{
		engine: gin.Default(),
		mr:     mr,
	}

	server.engine.GET("/api/data/:key", server.handleRead)
	server.engine.PUT("/api/data/:key", server.handleWrite)
	server.engine.DELETE("/api/data/:key", server.handleDelete)
	server.engine.GET("/api/data/stats", server.getStats)

	return server
}

func (server *RestServer) Run(port string) {
	server.engine.Run(fmt.Sprintf(":%s", port))
}
