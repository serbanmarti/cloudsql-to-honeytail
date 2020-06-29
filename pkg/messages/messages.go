package messages

import (
	"time"
)

// ParsedMessage holds the relevant details from a parsed Cloud Subscription message
type ParsedMessage struct {
	TextPayload string    `json:"textPayload"`
	Timestamp   time.Time `json:"timestamp"`
}
