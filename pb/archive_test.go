package pb

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Protobuf archive extraction", func() {
	Context("truncateProtoDirectories", func() {
		It("strips path down to root dir", func() {
			files := map[string]string{
				"batchcorp-schemas-5333a67203ba048fccd79e38d9434e3bc68baa10/events/testing.proto": "testing",
			}

			want := map[string]string{
				"account.proto": "testing",
			}

			Expect(truncateProtoDirectories(files, "events/")).To(Equal(want))
		})
	})
})
