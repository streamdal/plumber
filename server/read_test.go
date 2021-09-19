package server

import (
	"context"
	"sync"

	"github.com/batchcorp/plumber/server/types"

	uuid "github.com/satori/go.uuid"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber/config"
	"github.com/batchcorp/plumber/embed/etcd/etcdfakes"
)

var _ = Describe("Read", func() {
	var p *Server

	BeforeEach(func() {
		fakeEtcd := &etcdfakes.FakeIEtcd{}

		p = &Server{
			Etcd:      fakeEtcd,
			AuthToken: "batchcorp",
			PersistentConfig: &config.Config{
				ReadsMutex: &sync.RWMutex{},
				Reads:      map[string]*types.Read{},
			},
			Log: logrus.NewEntry(logger),
		}
	})

	Context("GetAllReads", func() {
		It("checks auth token", func() {
			_, err := p.GetAllReads(context.Background(), &protos.GetAllReadsRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
		})

		It("returns all reads", func() {
			for i := 0; i < 10; i++ {
				r := &protos.Read{
					Id: uuid.NewV4().String(),
				}

				p.PersistentConfig.SetRead(r.Id, &types.Read{ReadOptions: r})
			}

			resp, err := p.GetAllReads(context.Background(), &protos.GetAllReadsRequest{
				Auth: &common.Auth{Token: "batchcorp"},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(len(resp.Read)).To(Equal(10))
		})
	})

	Context("StartRead", func() {
		It("checks auth token", func() {
			err := p.StartRead(&protos.StartReadRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			}, nil)

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
		})

		It("attaches to a read", func() {
			// TODO: after refactor is merged
		})
	})

	Context("CreateRead", func() {
		It("checks auth token", func() {
			_, err := p.CreateRead(context.Background(), &protos.CreateReadRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
		})

		It("creates a read", func() {
			// TODO: after refactor is merged
		})
	})

	Context("StopRead", func() {
		It("checks auth token", func() {
			_, err := p.StopRead(context.Background(), &protos.StopReadRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
		})

		It("stops a read", func() {
			readID := uuid.NewV4().String()

			ctx, cancelFunc := context.WithCancel(context.Background())

			read := &types.Read{
				ReadOptions: &protos.Read{
					Active: true,
				},
				ContextCxl: ctx,
				CancelFunc: cancelFunc,
			}

			p.PersistentConfig.SetRead(readID, read)

			resp, err := p.StopRead(context.Background(), &protos.StopReadRequest{
				Auth:   &common.Auth{Token: "batchcorp"},
				ReadId: readID,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(resp.Status.Code).To(Equal(common.Code_OK))
			Expect(read.ReadOptions.Active).To(BeFalse())
		})
	})

	Context("ResumeRead", func() {
		It("checks auth token", func() {
			_, err := p.ResumeRead(context.Background(), &protos.ResumeReadRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
		})

		It("resumes a read", func() {
			// TODO: after refactor is merged
		})
	})

	Context("DeleteRead", func() {
		It("checks auth token", func() {
			_, err := p.DeleteRead(context.Background(), &protos.DeleteReadRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(ErrInvalidToken.Error()))
		})

		It("deletes a read", func() {
			readID := uuid.NewV4().String()

			ctx, cancelFunc := context.WithCancel(context.Background())

			read := &types.Read{
				ReadOptions: &protos.Read{
					Active: true,
				},
				ContextCxl: ctx,
				CancelFunc: cancelFunc,
			}

			p.PersistentConfig.SetRead(readID, read)

			resp, err := p.DeleteRead(context.Background(), &protos.DeleteReadRequest{
				Auth:   &common.Auth{Token: "batchcorp"},
				ReadId: readID,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(resp.Status.Code).To(Equal(common.Code_OK))
			Expect(len(p.PersistentConfig.Reads)).To(Equal(0))
		})
	})
})
