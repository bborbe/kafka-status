// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package status

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// App for display Kafka status information
type App struct {
	Port         int
	KafkaBrokers string
}

// Validate all required parameters are set.
func (a *App) Validate() error {
	if a.Port <= 0 {
		return errors.New("Port missing")
	}
	if a.KafkaBrokers == "" {
		return errors.New("KafkaBrokers missing")
	}
	return nil
}

// Run the status app.
func (a *App) Run(ctx context.Context) error {
	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/healthz", a.check)
	router.HandleFunc("/readiness", a.check)
	router.HandleFunc("/", a.status)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", a.Port),
		Handler: router,
	}
	go func() {
		select {
		case <-ctx.Done():
			if err := server.Shutdown(ctx); err != nil {
				glog.Warningf("shutdown failed: %v", err)
			}
		}
	}()
	return server.ListenAndServe()
}

func (a *App) check(resp http.ResponseWriter, req *http.Request) {
	resp.WriteHeader(http.StatusOK)
	fmt.Fprint(resp, "OK")
}

func (a *App) status(resp http.ResponseWriter, req *http.Request) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	resp.Header().Set("Content-Type", "text/plain")

	client, err := sarama.NewClient(strings.Split(a.KafkaBrokers, ","), config)
	if err != nil {
		fmt.Fprintf(resp, "create kafka client failed: %v", err)
		return
	}
	defer client.Close()

	fmt.Fprintln(resp, "Brokers:")
	for _, broker := range client.Brokers() {
		fmt.Fprintf(resp, "- ID: %v Addr: %v\n", broker.ID(), broker.Addr())
	}
	fmt.Fprintln(resp)

	fmt.Fprintln(resp, "Controller:")
	controller, err := client.Controller()
	fmt.Fprintf(resp, "- ID: %v\n", controller.ID())
	fmt.Fprintf(resp, "- Addr: %v\n", controller.Addr())
	fmt.Fprintln(resp)

	topics, err := client.Topics()
	if err != nil {
		fmt.Fprintf(resp, "get kafka topics failed: %v", err)
		return
	}
	fmt.Fprintln(resp, "Topics:")

	sort.Strings(topics)

	for _, topic := range topics {
		fmt.Fprintf(resp, "- %v (", topic)
		partitions, err := client.Partitions(topic)
		if err != nil {
			fmt.Fprintf(resp, "get partitions for topic %v failed: %v", topic, err)
			return
		}
		for i, partition := range partitions {
			offset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				fmt.Fprintf(resp, "get offset for topic %s and partition %d failed: %v", topic, partition, err)
				return
			}
			if i == 0 {
				fmt.Fprintf(resp, "%d=%d", partition, offset)
			} else {
				fmt.Fprintf(resp, ",%d=%d", partition, offset)
			}
		}
		fmt.Fprintln(resp, ")")
	}
	fmt.Fprintln(resp)
}
