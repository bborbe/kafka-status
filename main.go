// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	flag "github.com/bborbe/flagenv"
	"github.com/bborbe/kafka-status/status"
	"github.com/golang/glog"
)

func main() {
	defer glog.Flush()
	glog.CopyStandardLogTo("info")
	runtime.GOMAXPROCS(runtime.NumCPU())

	app := &status.App{}
	flag.IntVar(&app.Port, "port", 9003, "port to listen")
	flag.StringVar(&app.KafkaBrokers, "kafka-brokers", "", "kafka brokers")

	_ = flag.Set("logtostderr", "true")
	flag.Parse()

	glog.V(0).Infof("Parameter Metrics-Port: %d", app.Port)
	glog.V(0).Infof("Parameter KafkaBrokers: %s", app.KafkaBrokers)

	if err := app.Validate(); err != nil {
		glog.Exitf("validate app failed: %v", err)
	}

	ctx := contextWithSig(context.Background())

	glog.V(0).Infof("app started")
	if err := app.Run(ctx); err != nil {
		glog.Exitf("app failed: %+v", err)
	}
	glog.V(0).Infof("app finished")
}

func contextWithSig(ctx context.Context) context.Context {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-signalCh:
		case <-ctx.Done():
		}
	}()

	return ctxWithCancel
}
