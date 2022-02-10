package uierrors

//
//import (
//	"context"
//	"fmt"
//	"io/ioutil"
//	"sync"
//	"time"
//
//	"github.com/golang/protobuf/proto"
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//	uuid "github.com/satori/go.uuid"
//	"github.com/sirupsen/logrus"
//	"go.etcd.io/etcd/api/v3/mvccpb"
//	clientv3 "go.etcd.io/etcd/client/v3"
//
//	"github.com/batchcorp/plumber-schemas/build/go/protos"
//
//	"github.com/batchcorp/plumber/embed/bus/etcdfakes"
//)
//
//var _ = Describe("uierrors", func() {
//	var u *UIErrors
//
//	BeforeEach(func() {
//		u = &UIErrors{
//			Config: &Config{
//				EtcdService: &etcdfakes.FakeIEtcd{},
//			},
//			AttachedClientsMtx: &sync.RWMutex{},
//			AttachedClients:    make(map[string]*AttachedStream),
//			log:                logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
//		}
//	})
//
//	Context("validateConfig", func() {
//		It("validates etcd", func() {
//			err := validateConfig(&Config{})
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(Equal(ErrMissingEtcd.Error()))
//		})
//
//		It("passes validation", func() {
//			cfg := &Config{
//				EtcdService: &etcdfakes.FakeIEtcd{},
//			}
//
//			err := validateConfig(cfg)
//			Expect(err).ToNot(HaveOccurred())
//		})
//	})
//
//	Context("AddError", func() {
//		It("sends to attached client", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			u.EtcdService = fakeEtcd
//
//			ch := make(chan *protos.ErrorMessage, 1)
//			defer close(ch)
//
//			u.AttachedClients["test"] = &AttachedStream{MessageCh: ch}
//
//			msg := &protos.ErrorMessage{
//				Resource:   "read",
//				ResourceId: uuid.NewV4().String(),
//				Error:      "test error",
//			}
//
//			err := u.AddError(msg)
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(fakeEtcd.PutCallCount()).To(Equal(0))
//			Expect(ch).Should(Receive())
//		})
//	})
//
//	Context("ConnectClient", func() {
//		It("", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			fakeEtcd.GetStub = func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
//				return &clientv3.GetResponse{
//					Kvs: make([]*mvccpb.KeyValue, 0),
//				}, nil
//			}
//
//			u.EtcdService = fakeEtcd
//
//			Expect(len(u.AttachedClients)).To(Equal(0))
//			stream := u.ConnectClient("test")
//			Expect(len(u.AttachedClients)).To(Equal(1))
//			Expect(u.AttachedClients["test"]).To(Equal(stream))
//		})
//	})
//
//	Context("GetHistory", func() {
//		It("returns slice of protos.ErrorMessage", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			fakeEtcd.GetStub = func(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
//				msg := &protos.ErrorMessage{
//					Resource:   "read",
//					ResourceId: "1",
//					Error:      "test",
//					Timestamp:  time.Now().UTC().UnixNano(),
//				}
//
//				data, _ := proto.Marshal(msg)
//
//				r := &clientv3.GetResponse{
//					Kvs: make([]*mvccpb.KeyValue, 0),
//				}
//
//				r.Kvs = append(r.Kvs, &mvccpb.KeyValue{
//					Key:   []byte(fmt.Sprintf("%d", time.Now().UTC().UnixNano())),
//					Value: data,
//				})
//
//				return r, nil
//			}
//
//			u.EtcdService = fakeEtcd
//
//			history, err := u.GetHistory(context.Background())
//			Expect(err).ToNot(HaveOccurred())
//			Expect(len(history)).To(Equal(1))
//		})
//	})
//
//	Context("DisconnectClient", func() {
//		It("removes the client", func() {
//			ch := make(chan *protos.ErrorMessage, 1)
//
//			u.AttachedClients["test"] = &AttachedStream{MessageCh: ch}
//
//			u.DisconnectClient("test")
//
//			Expect(len(u.AttachedClients)).To(Equal(0))
//			Expect(ch).To(BeClosed())
//		})
//	})
//
//	Context("validateError", func() {
//		It("validates missing error", func() {
//			err := validateError(&protos.ErrorMessage{})
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(Equal(ErrMissingError.Error()))
//		})
//
//		It("validates missing resource", func() {
//			err := validateError(&protos.ErrorMessage{
//				Error: "test",
//			})
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(Equal(ErrMissingResource.Error()))
//		})
//
//		It("validates missing resource id", func() {
//			err := validateError(&protos.ErrorMessage{
//				Error:    "test",
//				Resource: "read",
//			})
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(Equal(ErrMissingResourceID.Error()))
//		})
//
//		It("passes validation", func() {
//			err := validateError(&protos.ErrorMessage{
//				Error:      "test",
//				Resource:   "read",
//				ResourceId: uuid.NewV4().String(),
//			})
//			Expect(err).ToNot(HaveOccurred())
//		})
//	})
//})
