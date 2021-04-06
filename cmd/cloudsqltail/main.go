package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"

	"cloudsqltail/messages"
)

var (
	// Flags used for configuration
	flagProject           = flag.String("project", "", "GCP Project ID")
	flagSubscription      = flag.String("subscription", "", "GCP Pub/Sub Subscription name")
	flagReceiveGoroutines = flag.Int(
		"recv-routines",
		runtime.NumCPU(),
		"Number of goroutines to use to receive messages from the Pub/Sub Subscription. [default: runtime.NumCPUs()]",
	)
	flagFlushInterval = flag.Duration(
		"flush-interval",
		5*time.Second,
		"Time between flushes of message slice to STDOUT.",
	)

	// Used to store messages until they are flushed to Honeycomb
	globalMessages []messages.ParsedMessage

	// Mutex used to protect the global messages slice
	mx sync.Mutex
)

func main() {
	// Parse input flags
	err := parseFlags()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}

	// Create the process context
	ctx := context.Background()

	// Create the subscription to Pub/Sub
	sub, err := subscribeToPubSub(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}

	// Start the messages flush mechanism in a separate routine
	go flushMessages(*flagFlushInterval)

	// Serve the HTTP server in a separate routine
	go serveHttpServer()

	// Start a blocking call that waits to receive new messages
	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// Parse the received message
		parseMessage(msg.Data)

		// Acknowledge that the message was received
		msg.Ack()
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}

// parseFlags given as input for missing or incorrect data
func parseFlags() error {
	flag.Parse()

	switch "" {
	case *flagProject:
		return errors.New("must provide -project")
	case *flagSubscription:
		return errors.New("must provide -subscription")
	}

	if *flagReceiveGoroutines < 1 {
		_, err := fmt.Fprintf(
			os.Stdout,
			`WARNING: Cannot have "%d" routines. Using default value of "%d"!`,
			*flagReceiveGoroutines, pubsub.DefaultReceiveSettings.NumGoroutines,
		)
		if err != nil {
			return err
		}
	}

	if *flagFlushInterval < 1 {
		return errors.New(fmt.Sprintf("flush internal '%s' must be > 0", *flagFlushInterval))
	} else if *flagFlushInterval < time.Second {
		_, err := fmt.Fprintf(
			os.Stdout,
			`WARNING: Using an small flush interval may result in more out-of-order output. Are you sure you didn't mean "%ds"?`,
			*flagFlushInterval,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// serveHttpServer for liveliness/readiness check by GKE probe
func serveHttpServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := fmt.Fprint(w, "Alive!")
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "could not return HTTP response: %s", err.Error())
			os.Exit(1)
		}
	})
	err := http.ListenAndServe(":5000", nil)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "could not start HTTP server: %s", err.Error())
		os.Exit(1)
	}
}

// parseMessage that is received, by taking the given JSON data,
// parsing it and appending the ParsedMessage to the global messages slice
func parseMessage(data []byte) {
	var pm messages.ParsedMessage

	// Parse the JSON data
	if err := json.Unmarshal(data, &pm); err != nil {
		// Ignore it if it is erroneous
		return
	}

	// Get a lock on the messages slice
	mx.Lock()
	defer mx.Unlock()

	// Add the new message to the slice
	globalMessages = append(globalMessages, pm)
}

// subscribeToPubSub subscription in order to receive messages from logs
func subscribeToPubSub(ctx context.Context) (*pubsub.Subscription, error) {
	// Create a new Pub/Sub client for the given GCP project
	c, err := pubsub.NewClient(ctx, *flagProject)
	if err != nil {
		return nil, err
	}

	// Subscribe into the given Pub/Sub subscription
	sub := c.Subscription(*flagSubscription)
	sub.ReceiveSettings.NumGoroutines = *flagReceiveGoroutines

	return sub, nil
}

// flushMessages will flush the message slice on every tick
func flushMessages(d time.Duration) {
	// Create a ticker for that helps us wait 'dur' to flush
	tick := time.NewTicker(d)

	for {
		// Wait for the next tick
		<-tick.C

		// Get a lock on the messages slice
		mx.Lock()

		// If no messages available, ignore
		if len(globalMessages) > 0 {
			// Sort the messages by timestamp
			sort.Slice(globalMessages, func(i, j int) bool {
				return globalMessages[i].Timestamp.Before(globalMessages[j].Timestamp)
			})

			// Run through all messages
			for i := range globalMessages {
				msg := &globalMessages[i]

				if msg.TextPayload != "" {
					// Print the timestamp if we have the first line in a message sequence
					if msg.TextPayload[0] == '[' {
						timestamp := globalMessages[i].Timestamp.Format("2006-01-02 15:04:05.999999999 UTC") // format that pg uses
						fmt.Printf("[%s]: %s\n", timestamp, globalMessages[i].TextPayload)
					} else {
						fmt.Println(globalMessages[i].TextPayload)
					}
				}
			}

			// Reset the global messages slice, pre-allocating enough capacity to
			// fit the same number of messages as we saw last time.
			globalMessages = make([]messages.ParsedMessage, 0, len(globalMessages))
		}

		// Release the lock on the messages slice
		mx.Unlock()
	}
}
