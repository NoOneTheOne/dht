package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/NoOneTheOne/dht/pkg/mysql"
)

const maxQueryLen = 200

func main() {
	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()
	gin.DefaultWriter = io.MultiWriter(os.Stdout)
	router := gin.New()
	router.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("{\"@timestamp\":\"%s\",\"remoteAddr\":\"%s\",\"request_method\":\"%s\",\"request_uri\":\"%s\",\"protocol\":\"%s\",\"status\":\"%d\",\"requestTime\":\"%s\",\"httpUserAgent\":\"%s\",\"Errormessage\":\"%s\"}\n",
			param.TimeStamp.Format(time.RFC3339),
			param.ClientIP,
			param.Method,
			param.Path,
			param.Request.Proto,
			param.StatusCode,
			param.Latency,
			param.Request.UserAgent(),
			param.ErrorMessage,
		)
	}))
	router.Use(gin.Recovery())

	router.GET("/search", func(c *gin.Context) {
		name := c.Query("name")
		if len(name) > maxQueryLen {
			c.JSON(http.StatusBadRequest, gin.H{"error": "query too long"})
			return
		}
		results, total, err := mysql.Search(name)
		if err != nil {
			log.Printf("search error: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"total": total,
			"items": results,
		})
	})

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	port := os.Getenv("SEARCH_API_PORT")
	if port == "" {
		port = "30080"
	}
	err := router.Run(":" + port)
	if err != nil {
		log.Fatalln(err)
	}
}
