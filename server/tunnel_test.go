package server

import (
	"context"
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
	"github.com/batchcorp/plumber-schemas/build/go/protos/args"
	"github.com/batchcorp/plumber-schemas/build/go/protos/common"
	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
	"github.com/batchcorp/plumber/actions/actionsfakes"
	"github.com/batchcorp/plumber/bus/busfakes"
	"github.com/batchcorp/plumber/config"
	stypes "github.com/batchcorp/plumber/server/types"
	"github.com/batchcorp/plumber/validate"
)

var _ = Describe("Tunnel", func() {

	var p *Server

	BeforeEach(func() {
		fakeBus := &busfakes.FakeIBus{}
		pConfig := &config.Config{
			Connections:      map[string]*stypes.Connection{},
			Tunnels:          map[string]*stypes.Tunnel{},
			TunnelsMutex:     &sync.RWMutex{},
			ConnectionsMutex: &sync.RWMutex{},
		}

		fakeActions := &actionsfakes.FakeIActions{}

		p = &Server{
			Bus:     fakeBus,
			Actions: fakeActions,

			AuthToken:        "streamdal",
			PersistentConfig: pConfig,
			Log:              logrus.NewEntry(logger),
		}
	})

	Context("GetAllTunnels", func() {
		It("check auth token", func() {
			_, err := p.CreateTunnel(context.Background(), &protos.CreateTunnelRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("returns all tunnels", func() {
			var inTunnels []*stypes.Tunnel

			for i := 0; i < 10; i++ {
				// set connection
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

				tunnelOpts := &opts.TunnelOptions{
					XActive:      false,
					XTunnelId:    uuid.NewV4().String(),
					ConnectionId: connID,
				}
				tunnelId := uuid.NewV4().String()
				tunnel := &stypes.Tunnel{
					Active:  false,
					Id:      tunnelId,
					Options: tunnelOpts,
				}
				p.PersistentConfig.SetTunnel(tunnelId, tunnel)
				inTunnels = append(inTunnels, tunnel)
			}

			resp, err := p.GetAllTunnels(context.Background(), &protos.GetAllTunnelsRequest{
				Auth: &common.Auth{Token: "streamdal"},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(len(resp.Opts)).To(Equal(10))

			for i := 0; i < 10; i++ {
				Expect(resp.Opts[i].XTunnelId, inTunnels[i].Id)
			}
		})
	})

	Context("DeleteTunnel", func() {
		It("check auth token", func() {
			_, err := p.DeleteTunnel(context.Background(), &protos.DeleteTunnelRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("delete a tunnel", func() {
			// set tunnel
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

			tunnelId := uuid.NewV4().String()
			tunnelOpts := &opts.TunnelOptions{
				XActive:      false,
				XTunnelId:    tunnelId,
				ConnectionId: connID,
			}

			tunnel := &stypes.Tunnel{
				Active:  false,
				Id:      tunnelId,
				Options: tunnelOpts,
			}
			p.PersistentConfig.SetTunnel(tunnelId, tunnel)

			delRequest := &protos.DeleteTunnelRequest{
				Auth:     &common.Auth{Token: "streamdal"},
				TunnelId: tunnelId,
			}

			bFake := &busfakes.FakeIBus{}
			p.Bus = bFake
			aFake := &actionsfakes.FakeIActions{}
			aFake.DeleteTunnelStub = func(ctx context.Context, s string) error {
				return nil
			}
			p.Actions = aFake

			_, err := p.DeleteTunnel(context.Background(), delRequest)
			Expect(err).ToNot(HaveOccurred())
			Expect(aFake.DeleteTunnelCallCount()).To(Equal(1))
		})

	})
})
