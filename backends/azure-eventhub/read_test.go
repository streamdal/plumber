package azure_eventhub

//
//import (
//	"context"
//	"io/ioutil"
//
//	eventhub "github.com/Azure/azure-event-hubs-go/v3"
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//	"github.com/sirupsen/logrus"
//
//	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
//	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
//	"github.com/batchcorp/plumber-schemas/build/go/protos/records"
//	"github.com/batchcorp/plumber/tools/toolsfakes"
//)
//
//var _ = Describe("Azure Event Hub", func() {
//	Context("Read", func() {
//		It("reads message", func() {
//			fakeEH := &toolsfakes.FakeIEventhub{}
//
//			fakeEH.GetRuntimeInformationStub = func(context.Context) (*eventhub.HubRuntimeInformation, error) {
//				return &eventhub.HubRuntimeInformation{
//					PartitionCount: 1,
//					PartitionIDs:   []string{"1"},
//				}, nil
//			}
//
//			fakeEH.ReceiveStub = func(context.Context, string, eventhub.Handler, ...eventhub.ReceiveOption) (*eventhub.ListenerHandle, error) {
//				ctx, _, handler, _ := fakeEH.ReceiveArgsForCall(0)
//
//				handler(ctx, &eventhub.Event{
//					ID:               "1",
//					Data:             []byte("test"),
//					SystemProperties: nil,
//				})
//
//				return nil, nil
//			}
//
//			h := &AzureEventHub{
//				connOpts: nil,
//				connArgs: nil,
//				client:   fakeEH,
//				log:      logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
//			}
//
//			readOpts := &opts.ReadOptions{
//				AzureEventHub: &opts.ReadGroupAzureEventHubOptions{
//					Args: &args.AzureEventHubReadArgs{},
//				},
//				Continuous: false,
//			}
//
//			resultsChan := make(chan *records.ReadRecord, 1)
//			errorChan := make(chan *records.ErrorRecord, 1)
//
//			err := h.Read(context.Background(), readOpts, resultsChan, errorChan)
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resultsChan).Should(Receive())
//			Expect(errorChan).ShouldNot(Receive())
//		})
//	})
//})
