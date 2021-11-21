package mqtt

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("MQTT Backend", func() {
	var dynamicOpts *opts.DynamicOptions

	BeforeEach(func() {
		dynamicOpts = &opts.DynamicOptions{
			Mqtt: &opts.DynamicGroupMQTTOptions{
				Args: &args.MQTTWriteArgs{
					Topic:               "test",
					WriteTimeoutSeconds: 1,
				},
			},
		}
	})

	Context("validateDynamicOptions", func() {
		It("validates nil dynamic options", func() {
			err := validateDynamicOptions(nil)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyDynamicOpts))
		})
		It("validates nil backend group", func() {
			dynamicOpts.Mqtt = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendGroup))
		})
		It("validates empty backend args", func() {
			dynamicOpts.Mqtt.Args = nil
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(validate.ErrEmptyBackendArgs))
		})
		It("validates empty topic", func() {
			dynamicOpts.Mqtt.Args.Topic = ""
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(ErrEmptyTopic))
		})
		It("passes validation", func() {
			err := validateDynamicOptions(dynamicOpts)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
