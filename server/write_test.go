package server

import (
	"github.com/jhump/protoreflect/desc"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Write", func() {
	Context("generateWriteValue", func() {
		It("returns original data when no encoding options passed", func() {
			data := []byte(`test`)

			wv, err := generateWriteValue(&desc.MessageDescriptor{}, nil, data)

			Expect(err).ToNot(HaveOccurred())
			Expect(wv).To(Equal(data))
		})
	})
})
