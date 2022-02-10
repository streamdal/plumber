package server

//
//import (
//	"context"
//	"io/ioutil"
//	"sync"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//	uuid "github.com/satori/go.uuid"
//	"github.com/sirupsen/logrus"
//
//	"github.com/batchcorp/plumber-schemas/build/go/protos"
//	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
//	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
//
//	"github.com/batchcorp/plumber/config"
//	"github.com/batchcorp/plumber/embed/bus/etcdfakes"
//	"github.com/batchcorp/plumber/server/types"
//	"github.com/batchcorp/plumber/validate"
//)
//
//var _ = Describe("Composite view handlers", func() {
//
//	var p *Server
//
//	BeforeEach(func() {
//		//fakeEtcd := &etcdfakes.FakeIEtcd{}
//
//		p = &Server{
//			Etcd:      fakeEtcd,
//			AuthToken: "batchcorp",
//			PersistentConfig: &config.Config{
//				ReadsMutex:      &sync.RWMutex{},
//				CompositesMutex: &sync.RWMutex{},
//				Reads:           make(map[string]*types.Read),
//				Composites:      make(map[string]*opts.Composite),
//			},
//			Log: logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
//		}
//	})
//
//	Context("GetComposite", func() {
//		It("checks auth token", func() {
//			_, err := p.GetComposite(context.Background(), &protos.GetCompositeRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
//		})
//
//		It("gets a single composite view", func() {
//			id := uuid.NewV4().String()
//
//			composite := &opts.Composite{
//				XId:     id,
//				ReadIds: []string{uuid.NewV4().String(), uuid.NewV4().String()},
//			}
//			p.PersistentConfig.SetComposite(id, composite)
//
//			resp, err := p.GetComposite(context.Background(), &protos.GetCompositeRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//				Id:   id,
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resp.Composite.XId).To(Equal(id))
//		})
//	})
//
//	Context("GetAllComposites", func() {
//		It("checks auth token", func() {
//			_, err := p.GetAllComposites(context.Background(), &protos.GetAllCompositesRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
//		})
//		It("returns all composite views", func() {
//			for i := 0; i < 10; i++ {
//				composite := &opts.Composite{
//					XId:     uuid.NewV4().String(),
//					ReadIds: []string{uuid.NewV4().String(), uuid.NewV4().String()},
//				}
//				p.PersistentConfig.SetComposite(composite.XId, composite)
//			}
//
//			resp, err := p.GetAllComposites(context.Background(), &protos.GetAllCompositesRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(len(resp.Composites)).To(Equal(10))
//		})
//	})
//
//	Context("CreateComposite", func() {
//		It("checks auth token", func() {
//			_, err := p.CreateComposite(context.Background(), &protos.CreateCompositeRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
//		})
//
//		It("creates a composite view", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			p.Etcd = fakeEtcd
//
//			composite := &opts.Composite{
//				XId:     uuid.NewV4().String(),
//				ReadIds: []string{uuid.NewV4().String(), uuid.NewV4().String()},
//			}
//
//			resp, err := p.CreateComposite(context.Background(), &protos.CreateCompositeRequest{
//				Auth:      &common.Auth{Token: "batchcorp"},
//				Composite: composite,
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resp.Composite.ReadIds).To(Equal(composite.ReadIds))
//			Expect(fakeEtcd.PutCallCount()).To(Equal(1))
//			Expect(fakeEtcd.PublishCreateCompositeCallCount()).To(Equal(1))
//		})
//	})
//
//	Context("UpdateComposite", func() {
//		It("checks auth token", func() {
//			_, err := p.UpdateComposite(context.Background(), &protos.UpdateCompositeRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
//		})
//
//		It("updates a composite view", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			p.Etcd = fakeEtcd
//
//			compositeID := uuid.NewV4().String()
//
//			composite := &opts.Composite{
//				XId:     compositeID,
//				ReadIds: []string{uuid.NewV4().String(), uuid.NewV4().String()},
//			}
//			p.PersistentConfig.SetComposite(composite.XId, composite)
//
//			newComposite := &opts.Composite{
//				XId:     compositeID,
//				ReadIds: []string{uuid.NewV4().String(), uuid.NewV4().String()},
//			}
//
//			resp, err := p.UpdateComposite(context.Background(), &protos.UpdateCompositeRequest{
//				Auth:      &common.Auth{Token: "batchcorp"},
//				Id:        compositeID,
//				Composite: newComposite,
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resp.Composite.ReadIds).To(Equal(newComposite.ReadIds))
//			Expect(fakeEtcd.PutCallCount()).To(Equal(1))
//			Expect(fakeEtcd.PublishUpdateCompositeCallCount()).To(Equal(1))
//		})
//	})
//
//	Context("DeleteComposite", func() {
//		It("checks auth token", func() {
//			_, err := p.DeleteComposite(context.Background(), &protos.DeleteCompositeRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
//		})
//
//		It("returns an error when composite view not found", func() {
//			_, err := p.DeleteComposite(context.Background(), &protos.DeleteCompositeRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//				Id:   uuid.NewV4().String(),
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(Equal(validate.ErrCompositeNotFound.Error()))
//		})
//
//		It("deletes a composite view", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			p.Etcd = fakeEtcd
//
//			compositeID := uuid.NewV4().String()
//
//			composite := &opts.Composite{
//				XId:     compositeID,
//				ReadIds: []string{uuid.NewV4().String(), uuid.NewV4().String()},
//			}
//			p.PersistentConfig.SetComposite(compositeID, composite)
//
//			resp, err := p.DeleteComposite(context.Background(), &protos.DeleteCompositeRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//				Id:   compositeID,
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resp.Status.Code).To(Equal(common.Code_OK))
//			Expect(fakeEtcd.DeleteCallCount()).To(Equal(1))
//			Expect(fakeEtcd.PublishDeleteCompositeCallCount()).To(Equal(1))
//		})
//	})
//})
