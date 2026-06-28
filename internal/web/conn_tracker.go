package web

import (
	"time"
)

type Connection struct {
	ClientAddr     string
	StartTime      time.Time
	BytesStreamed  int64
	RangeStart     int64
	RangeEnd       int64
	Completed      bool
	LastActivity   time.Time
	DisconnectTime *time.Time
	Error          error
}

type ConnTracker struct{}

func NewConnTracker() *ConnTracker {
	return &ConnTracker{}
}

func (c *ConnTracker) DetectReconnection(msgID int, addr string, start int64) (bool, *Connection) {
	return false, nil
}

func (c *ConnTracker) RegisterConnection(msgID int, addr string, start, end int64) string {
	return ""
}

func (c *ConnTracker) MarkDisconnected(id string, bytes int64)    {}
func (c *ConnTracker) MarkCompleted(id string, bytes int64)       {}
func (c *ConnTracker) MarkError(id string, bytes int64, err error) {}

func (c *ConnTracker) GetActiveConnections() int {
	return 0
}

func (c *ConnTracker) GetStatistics() map[string]interface{} {
	return map[string]interface{}{
		"total_connections":    0,
		"completed_streams":    0,
		"disconnected_streams": 0,
		"errored_streams":      0,
		"total_gb_streamed":    0.0,
	}
}

func (c *ConnTracker) GetConnectionsByMessageID(msgID int) []Connection {
	return nil
}
