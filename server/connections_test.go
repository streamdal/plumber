package server

import (
	"context"
	"io/ioutil"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/actions"
	"github.com/batchcorp/plumber/bus/busfakes"
	"github.com/batchcorp/plumber/config"
	stypes "github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"
)

var logger = &logrus.Logger{Out: ioutil.Discard}

var _ = Describe("Connection", func() {

	var p *Server

	BeforeEach(func() {
		fakeBus := &busfakes.FakeIBus{}

		p = &Server{
			Bus:       fakeBus,
			AuthToken: "streamdal",
			PersistentConfig: &config.Config{
				ConnectionsMutex: &sync.RWMutex{},
				Connections:      map[string]*stypes.Connection{},
				TunnelsMutex:     &sync.RWMutex{},
				RelaysMutex:      &sync.RWMutex{},
			},
			Log: logrus.NewEntry(logger),
		}
	})

	Context("GetConnection", func() {
		It("check auth token", func() {
			_, err := p.GetConnection(context.Background(), &protos.GetConnectionRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("returns a specific connection", func() {
			connID := uuid.NewV4().String()

			conn := &opts.ConnectionOptions{
				Name:  "testing",
				Notes: "test connection",
				XId:   connID,
				Conn: &opts.ConnectionOptions_Kafka{Kafka: &args.KafkaConn{
					Address: []string{"127.0.0.1:9200"},
				}},
			}

			p.PersistentConfig.SetConnection(connID, &stypes.Connection{Connection: conn})

			resp, err := p.GetConnection(context.Background(), &protos.GetConnectionRequest{
				Auth:         &common.Auth{Token: "streamdal"},
				ConnectionId: connID,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(resp.GetOptions()).To(Equal(conn))
		})
	})

	Context("GetAllConnections", func() {
		It("check auth token", func() {
			_, err := p.GetConnection(context.Background(), &protos.GetConnectionRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("returns all specific connections", func() {

			for i := 0; i < 10; i++ {
				conn := &opts.ConnectionOptions{
					Name:  "testing",
					Notes: "test connection",
					XId:   uuid.NewV4().String(),
					Conn: &opts.ConnectionOptions_Kafka{Kafka: &args.KafkaConn{
						Address: []string{"127.0.0.1:9200"},
					}},
				}
				p.PersistentConfig.SetConnection(conn.XId, &stypes.Connection{Connection: conn})
			}

			resp, err := p.GetAllConnections(context.Background(), &protos.GetAllConnectionsRequest{
				Auth: &common.Auth{Token: "streamdal"},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(len(resp.Options)).To(Equal(10))
		})
	})

	Context("TestConnection", func() {
		It("check auth token", func() {
			_, err := p.TestConnection(context.Background(), &protos.TestConnectionRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})
		It("tests connection", func() {
			// TODO
		})
	})

	Context("CreateConnection", func() {
		It("check auth token", func() {
			_, err := p.CreateConnection(context.Background(), &protos.CreateConnectionRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("creates a connection", func() {
			conn := &opts.ConnectionOptions{
				Name:  "testing",
				Notes: "test connection",
				Conn: &opts.ConnectionOptions_Kafka{Kafka: &args.KafkaConn{
					Address: []string{"127.0.0.1:9200"},
				}},
			}

			resp, err := p.CreateConnection(context.Background(), &protos.CreateConnectionRequest{
				Auth:    &common.Auth{Token: "streamdal"},
				Options: conn,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(resp.ConnectionId).ToNot(BeEmpty())
		})
	})

	Context("UpdateConnection", func() {
		It("check auth token", func() {
			_, err := p.UpdateConnection(context.Background(), &protos.UpdateConnectionRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("update a connection", func() {
			connID := uuid.NewV4().String()

			fakeBus := &busfakes.FakeIBus{}
			p.Bus = fakeBus

			a, err := actions.New(&actions.Config{PersistentConfig: p.PersistentConfig})
			Expect(err).ToNot(HaveOccurred())
			p.Actions = a

			conn := &opts.ConnectionOptions{
				XId:   connID,
				Name:  "testing",
				Notes: "test connection",
				Conn: &opts.ConnectionOptions_Kafka{Kafka: &args.KafkaConn{
					Address: []string{"127.0.0.1:9200"},
				}},
			}

			p.PersistentConfig.SetConnection(connID, &stypes.Connection{Connection: conn})

			newConn := &opts.ConnectionOptions{
				XId:   connID,
				Name:  "updated",
				Notes: "test connection",
				Conn: &opts.ConnectionOptions_Kafka{Kafka: &args.KafkaConn{
					Address: []string{"127.0.0.1:9200"},
				}},
			}

			_, err = p.UpdateConnection(context.Background(), &protos.UpdateConnectionRequest{
				Auth:         &common.Auth{Token: "streamdal"},
				ConnectionId: connID,
				Options:      newConn,
			})
			Expect(err).ToNot(HaveOccurred())

			updateConn := p.PersistentConfig.GetConnection(connID)

			Expect(err).ToNot(HaveOccurred())
			Expect(updateConn.Connection).To(Equal(newConn))
			Expect(fakeBus.PublishUpdateConnectionCallCount()).To(Equal(1))
		})
	})

	Context("DeleteConnection", func() {
		It("check auth token", func() {
			_, err := p.DeleteConnection(context.Background(), &protos.DeleteConnectionRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("deletes a connection", func() {
			connID := uuid.NewV4().String()

			fakeBus := &busfakes.FakeIBus{}
			p.Bus = fakeBus

			conn := &opts.ConnectionOptions{
				XId:   connID,
				Name:  "testing",
				Notes: "test connection",
				Conn: &opts.ConnectionOptions_Kafka{Kafka: &args.KafkaConn{
					Address: []string{"127.0.0.1:9200"},
				}},
			}

			p.PersistentConfig.SetConnection(connID, &stypes.Connection{Connection: conn})

			resp, err := p.DeleteConnection(context.Background(), &protos.DeleteConnectionRequest{
				Auth:         &common.Auth{Token: "streamdal"},
				ConnectionId: connID,
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(resp.Status.Code).To(Equal(common.Code_OK))
			Expect(fakeBus.PublishDeleteConnectionCallCount()).To(Equal(1))
		})
	})
})
