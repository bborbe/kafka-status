// Copyright (c) 2018 Benjamin Borbe All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package status_test

import (
	"github.com/bborbe/kafka-status/status"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Status App", func() {
	var app *status.App
	BeforeEach(func() {
		app = &status.App{
			Port:         1337,
			KafkaBrokers: "kafka:9092",
		}
	})
	It("Validate without error", func() {
		Expect(app.Validate()).NotTo(HaveOccurred())
	})
	It("Validate returns error if port is 0", func() {
		app.Port = 0
		Expect(app.Validate()).To(HaveOccurred())
	})
	It("Validate returns error KafkaBrokers is empty", func() {
		app.KafkaBrokers = ""
		Expect(app.Validate()).To(HaveOccurred())
	})
})
