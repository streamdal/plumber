package server

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Decoding", func() {
	Context("truncateProtoDirectories", func() {
		It("Removes files outside of the root directory and cleans paths", func() {
			files := map[string]string{
				"batchcorp-schemas-861d51e1885eadaf17312d7af72ab78a72d16ffd/fakes/sample-proto/sample-message.proto": "",
				"batchcorp-schemas-861d51e1885eadaf17312d7af72ab78a72d16ffd/events/message.proto":                    "",
			}

			expected := map[string]string{
				"message.proto": "",
			}

			cleaned := truncateProtoDirectories(files, "events")
			Expect(cleaned).To(Equal(expected))
		})
	})

	Context("getMessageDescriptor", func() {
		It("returns nil,nil when no options passed", func() {
			p := &Server{}

			md, err := p.getMessageDescriptor(nil)

			Expect(err).ToNot(HaveOccurred())
			Expect(md).To(BeNil())
		})
	})
})
