package server

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/streamdal/plumber/validate"

	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
)

var _ = Describe("Server", func() {
	Context("CustomError", func() {
		It("Returns error wrapper", func() {
			err := CustomError(common.Code_INTERNAL, "something went wrong")
			Expect(err).To(BeAssignableToTypeOf(&ErrorWrapper{}))
		})
	})

	Context("validateAuth", func() {
		It("validates missing auth", func() {
			p := &Server{}

			err := p.validateAuth(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrMissingAuth))
		})

		It("validates token", func() {
			p := &Server{AuthToken: "foo"}

			err := p.validateAuth(&common.Auth{Token: "batch"})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrInvalidToken))
		})
	})
})
