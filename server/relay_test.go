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

var _ = Describe("Relay", func() {

	var p *Server

	BeforeEach(func() {
		fakeBus := &busfakes.FakeIBus{}
		pConfig := &config.Config{
			Connections:      map[string]*stypes.Connection{},
			Relays:           map[string]*stypes.Relay{},
			TunnelsMutex:     &sync.RWMutex{},
			RelaysMutex:      &sync.RWMutex{},
			ConnectionsMutex: &sync.RWMutex{},
		}

		fakeActions := &actionsfakes.FakeIActions{}

		p = &Server{
			Bus:     fakeBus,
			Actions: fakeActions,

			AuthToken:        "batchcorp",
			PersistentConfig: pConfig,
			Log:              logrus.NewEntry(logger),
		}
	})

	Context("DeleteRelay", func() {
		It("check auth token", func() {
			_, err := p.DeleteRelay(context.Background(), &protos.DeleteRelayRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("delete a relay", func() {
			// set relay
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

			relayOpts := &opts.RelayOptions{
				XActive:         false,
				XRelayId:        uuid.NewV4().String(),
				CollectionToken: "1234",
				ConnectionId:    connID,
			}
			relayId := uuid.NewV4().String()
			relay := &stypes.Relay{
				Active:  false,
				Id:      relayId,
				Options: relayOpts,
			}
			p.PersistentConfig.SetRelay(relayId, relay)

			request := &protos.GetRelayRequest{
				Auth:    &common.Auth{Token: "batchcorp"},
				RelayId: relayId,
			}

			getResp, _ := p.GetRelay(context.Background(), request)
			Expect(getResp.Opts.XRelayId).To(Equal(relayId))

			delResp, _ := p.Actions.DeleteRelay(context.Background(), relayId)
			Expect(delResp.Id).To(Equal(relayId))

			getDeletedResp, err := p.GetRelay(context.Background(), request)
			Expect(err).ToNot(HaveOccurred())
			// should not be able to get back the deleted relay
			Expect(getDeletedResp).To(Equal(nil))
		})

	})

	Context("GetAllRelays", func() {
		It("check auth token", func() {
			_, err := p.CreateRelay(context.Background(), &protos.CreateRelayRequest{
				Auth: &common.Auth{Token: "incorrect token"},
			})

			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(validate.ErrInvalidToken.Error()))
		})

		It("returns all relays", func() {
			var inRelays []*stypes.Relay

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

				relayOpts := &opts.RelayOptions{
					XActive:         false,
					XRelayId:        uuid.NewV4().String(),
					CollectionToken: "1234",
					ConnectionId:    connID,
				}
				relayId := uuid.NewV4().String()
				relay := &stypes.Relay{
					Active:  false,
					Id:      relayId,
					Options: relayOpts,
				}
				p.PersistentConfig.SetRelay(relayId, relay)
				inRelays = append(inRelays, relay)
			}

			resp, err := p.GetAllRelays(context.Background(), &protos.GetAllRelaysRequest{
				Auth: &common.Auth{Token: "batchcorp"},
			})

			Expect(err).ToNot(HaveOccurred())
			Expect(len(resp.Opts)).To(Equal(10))

			for i := 0; i < 10; i++ {
				Expect(resp.Opts[i].XRelayId, inRelays[i].Id)
			}
		})
	})
})
