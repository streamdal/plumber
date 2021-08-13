package config

import (
	"sync"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"

	"github.com/batchcorp/plumber/server/types"

	"github.com/batchcorp/plumber-schemas/build/go/protos"
)

var _ = Describe("Config", func() {
	Context("Connections", func() {
		cfg := &Config{
			ConnectionsMutex: &sync.RWMutex{},
			Connections:      make(map[string]*types.Connection),
		}
		It("Can save/get/delete", func() {
			id := uuid.NewV4().String()

			conn := &types.Connection{Connection: &protos.Connection{
				Id: id,
			}}

			// Save
			cfg.SetConnection(conn.Id, conn)

			// Get
			stored := cfg.GetConnection(conn.Id)
			Expect(stored).To(Equal(conn))

			// Delete
			cfg.DeleteConnection(conn.Id)
			notThere := cfg.GetConnection(conn.Id)
			Expect(notThere).To(BeNil())
		})
	})

	Context("Services", func() {
		cfg := &Config{
			ServicesMutex: &sync.RWMutex{},
			Services:      make(map[string]*types.Service),
		}
		It("Can save/get/delete", func() {
			id := uuid.NewV4().String()

			svc := &types.Service{Service: &protos.Service{
				Id: id,
			}}

			// Save
			cfg.SetService(svc.Id, svc)

			// Get
			stored := cfg.GetService(svc.Id)
			Expect(stored).To(Equal(svc))

			// Delete
			cfg.DeleteService(svc.Id)
			notThere := cfg.GetService(svc.Id)
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
				Id:     id,
				Config: &protos.Relay{},
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

	Context("Schemas", func() {
		cfg := &Config{
			SchemasMutex: &sync.RWMutex{},
			Schemas:      make(map[string]*types.Schema),
		}
		It("Can save/get/delete", func() {
			id := uuid.NewV4().String()

			schema := &types.Schema{Schema: &protos.Schema{
				Id: id,
			}}

			// Save
			cfg.SetSchema(schema.Id, schema)

			// Get
			stored := cfg.GetSchema(schema.Id)
			Expect(stored).To(Equal(schema))

			// Delete
			cfg.DeleteSchema(schema.Id)
			notThere := cfg.GetSchema(schema.Id)
			Expect(notThere).To(BeNil())
		})
	})

	Context("Reads", func() {
		cfg := &Config{
			ReadsMutex: &sync.RWMutex{},
			Reads:      make(map[string]*protos.Read),
		}
		It("Can save/get/delete", func() {
			id := uuid.NewV4().String()

			read := &protos.Read{
				Id: id,
			}

			// Save
			cfg.SetRead(read.Id, read)

			// Get
			stored := cfg.GetRead(read.Id)
			Expect(stored).To(Equal(read))

			// Delete
			cfg.DeleteRead(read.Id)
			notThere := cfg.GetRead(read.Id)
			Expect(notThere).To(BeNil())
		})
	})
})
