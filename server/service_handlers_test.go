package server

//import (
//	"context"
//	"sync"
//
//	uuid "github.com/satori/go.uuid"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//	"github.com/sirupsen/logrus"
//
//	"github.com/batchcorp/plumber/config"
//	"github.com/batchcorp/plumber/embed/etcd/etcdfakes"
//
//	"github.com/batchcorp/plumber-schemas/build/go/protos"
//	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
//)
//
//var _ = Describe("Services", func() {
//
//	var p *Server
//
//	BeforeEach(func() {
//		fakeEtcd := &etcdfakes.FakeIEtcd{}
//
//		p = &Server{
//			Etcd:      fakeEtcd,
//			AuthToken: "batchcorp",
//			PersistentConfig: &config.Config{
//				ServicesMutex: &sync.RWMutex{},
//				Services:      map[string]*protos.Service{},
//			},
//			Log: logrus.NewEntry(logger),
//		}
//	})
//
//	Context("GetService", func() {
//		It("checks auth token", func() {
//			_, err := p.GetService(context.Background(), &protos.GetServiceRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
//		})
//
//		It("gets a single service", func() {
//			svcID := uuid.NewV4().String()
//
//			svc := &protos.Service{
//				Id:      svcID,
//				Name:    "testing",
//				RepoUrl: "https://github.com/batchcorp/plumber",
//				OwnerId: uuid.NewV4().String(),
//			}
//			p.PersistentConfig.SetService(svcID, svc)
//
//			resp, err := p.GetService(context.Background(), &protos.GetServiceRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//				Id:   svcID,
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resp.Service.Id).To(Equal(svcID))
//		})
//	})
//
//	Context("GetAllServices", func() {
//		It("checks auth token", func() {
//			_, err := p.GetAllServices(context.Background(), &protos.GetAllServicesRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
//		})
//		It("returns all services", func() {
//			for i := 0; i < 10; i++ {
//				svc := &protos.Service{
//					Id:      uuid.NewV4().String(),
//					Name:    "testing",
//					RepoUrl: "https://github.com/batchcorp/plumber",
//					OwnerId: uuid.NewV4().String(),
//				}
//				p.PersistentConfig.SetService(svc.Id, svc)
//			}
//
//			resp, err := p.GetAllServices(context.Background(), &protos.GetAllServicesRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(len(resp.Services)).To(Equal(10))
//		})
//	})
//
//	Context("CreateService", func() {
//		It("checks auth token", func() {
//			_, err := p.CreateService(context.Background(), &protos.CreateServiceRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
//		})
//
//		It("creates a service", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			p.Etcd = fakeEtcd
//
//			svc := &protos.Service{
//				Name:    "testing",
//				RepoUrl: "https://github.com/batchcorp/plumber",
//				OwnerId: uuid.NewV4().String(),
//			}
//
//			resp, err := p.CreateService(context.Background(), &protos.CreateServiceRequest{
//				Auth:    &common.Auth{Token: "batchcorp"},
//				Service: svc,
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resp.Service.Name).To(Equal(svc.Name))
//			Expect(fakeEtcd.PutCallCount()).To(Equal(1))
//			Expect(fakeEtcd.PublishCreateServiceCallCount()).To(Equal(1))
//		})
//	})
//
//	Context("UpdateService", func() {
//		It("checks auth token", func() {
//			_, err := p.UpdateService(context.Background(), &protos.UpdateServiceRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
//		})
//		It("updates a service", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			p.Etcd = fakeEtcd
//
//			svcID := uuid.NewV4().String()
//
//			svc := &protos.Service{
//				Id:      svcID,
//				Name:    "testing",
//				RepoUrl: "https://github.com/batchcorp/plumber",
//				OwnerId: uuid.NewV4().String(),
//			}
//			p.PersistentConfig.SetService(svcID, svc)
//
//			newSvc := &protos.Service{
//				Id:      svc.Id,
//				Name:    "updated name",
//				RepoUrl: "https://github.com/batchcorp/plumber",
//				OwnerId: svc.OwnerId,
//			}
//
//			resp, err := p.UpdateService(context.Background(), &protos.UpdateServiceRequest{
//				Auth:    &common.Auth{Token: "batchcorp"},
//				Service: newSvc,
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resp.Service).To(Equal(newSvc))
//			Expect(fakeEtcd.PutCallCount()).To(Equal(1))
//			Expect(fakeEtcd.PublishUpdateServiceCallCount()).To(Equal(1))
//		})
//	})
//
//	Context("DeleteService", func() {
//		It("checks auth token", func() {
//			_, err := p.DeleteService(context.Background(), &protos.DeleteServiceRequest{
//				Auth: &common.Auth{Token: "incorrect token"},
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
//		})
//
//		It("returns an error when service not found", func() {
//			_, err := p.DeleteService(context.Background(), &protos.DeleteServiceRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//				Id:   uuid.NewV4().String(),
//			})
//
//			Expect(err).To(HaveOccurred())
//			Expect(err.Error()).To(Equal(ErrServiceNotFound.Error()))
//		})
//
//		It("deletes a service", func() {
//			fakeEtcd := &etcdfakes.FakeIEtcd{}
//			p.Etcd = fakeEtcd
//
//			svcID := uuid.NewV4().String()
//
//			svc := &protos.Service{
//				Id:      svcID,
//				Name:    "testing",
//				RepoUrl: "https://github.com/batchcorp/plumber",
//				OwnerId: uuid.NewV4().String(),
//			}
//			p.PersistentConfig.SetService(svcID, svc)
//
//			resp, err := p.DeleteService(context.Background(), &protos.DeleteServiceRequest{
//				Auth: &common.Auth{Token: "batchcorp"},
//				Id:   svcID,
//			})
//
//			Expect(err).ToNot(HaveOccurred())
//			Expect(resp.Status.Code).To(Equal(common.Code_OK))
//			Expect(fakeEtcd.DeleteCallCount()).To(Equal(1))
//			Expect(fakeEtcd.PublishDeleteServiceCallCount()).To(Equal(1))
//		})
//	})
//})
