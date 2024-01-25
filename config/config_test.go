package config

import (
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	"github.com/streamdal/plumber/server/types"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"
)

var _ = Describe("Options", func() {
	Context("Connections", func() {
		cfg := &Config{
			ConnectionsMutex: &sync.RWMutex{},
			Connections:      make(map[string]*types.Connection),
		}
		It("Can save/get/delete", func() {
			id := uuid.NewV4().String()

			conn := &types.Connection{
				Connection: &opts.ConnectionOptions{
					XId: id,
				},
			}

			// Save
			cfg.SetConnection(conn.Connection.XId, conn)

			// Get
			stored := cfg.GetConnection(conn.Connection.XId)
			Expect(stored).To(Equal(conn))

			// Delete
			cfg.DeleteConnection(conn.Connection.XId)
			notThere := cfg.GetConnection(conn.Connection.XId)
			Expect(notThere).To(BeNil())
		})
	})

	Context("Relays", func() {
		cfg := &Config{
			RelaysMutex: &sync.RWMutex{},
			Relays:      make(map[string]*types.Relay),
		}
		It("Can save/get/delete", func() {
			id := uuid.NewV4().String()

			// TODO: this needs an ID
			relay := &types.Relay{
				Id:      id,
				Options: &opts.RelayOptions{},
			}

			// Save
			cfg.SetRelay(relay.Id, relay)

			// Get
			stored := cfg.GetRelay(relay.Id)
			Expect(stored).To(Equal(relay))

			// Delete
			cfg.DeleteRelay(relay.Id)
			notThere := cfg.GetRelay(relay.Id)
			Expect(notThere).To(BeNil())
		})
	})
})
