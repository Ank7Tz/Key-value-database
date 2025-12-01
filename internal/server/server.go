package server

import (
	"fmt"
	"key_value_store/internal/raft"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

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

	sc := ctx.DefaultQuery("strong_consistency", "false")

	flag, err := strconv.ParseBool(sc)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, ReadResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	value, err := server.mr.Read(key, flag)
	if err != nil {
		if strings.Contains(err.Error(), "key not found") {
			ctx.JSON(http.StatusNotFound, ReadResponse{
				Success: false,
				Error:   err.Error(),
			})
		} else {
			ctx.JSON(http.StatusInternalServerError, ReadResponse{
				Success: false,
				Error:   err.Error(),
			})
		}
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
		if strings.Contains(err.Error(), "key not found") {
			ctx.JSON(http.StatusNotFound, DeleteResponse{
				Success: false,
				Error:   err.Error(),
			})
		} else {
			ctx.JSON(http.StatusInternalServerError, DeleteResponse{
				Success: false,
				Error:   err.Error(),
			})
		}
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

func (server *RestServer) Stop(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, "")
	go func() {
		time.Sleep(2 * time.Second)
		fmt.Println("stopping server")
		os.Exit(1)
	}()
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
	server.engine.GET("/api/stop", server.Stop)

	return server
}

func (server *RestServer) Run(port string) {
	server.engine.Run(fmt.Sprintf(":%s", port))
}
