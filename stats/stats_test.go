package stats

import (
	"context"
	"errors"
	"io/ioutil"
	"sync"
	"time"

	"github.com/nakabonne/tstorage"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/plumber-schemas/build/go/protos/opts"

	"github.com/batchcorp/plumber/tools"
)

var _ = Describe("Stats counters", func() {
	var s *Stats
	var cancelFunc context.CancelFunc
	var ctx context.Context

	BeforeEach(func() {
		ctx, cancelFunc = context.WithCancel(context.Background())
		defer cancelFunc()

		s = &Stats{
			Config: &Config{
				FlushInterval:      time.Second,
				ServiceShutdownCtx: ctx,
			},
			countersMtx: &sync.RWMutex{},
			counters:    make(map[string]*Counter),
			storage:     &tools.FakeStorage{},
			log:         logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard}),
		}
	})

	AfterEach(func() {
		cancelFunc()
	})

	Context("Service", func() {
		Context("validateConfig", func() {
			It("validates FlushInterval", func() {
				cfg := &Config{
					FlushInterval:      0,
					ServiceShutdownCtx: context.Background(),
				}

				err := validateConfig(cfg)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(ErrInvalidFlushInterval.Error()))
			})

			It("validates ServiceShutdownCtx", func() {
				cfg := &Config{
					FlushInterval: time.Second * 1,
				}

				err := validateConfig(cfg)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(ErrMissingShutdownCtx.Error()))
			})

			It("validates ErrMissingTSStoragePath", func() {
				cfg := &Config{
					FlushInterval:      time.Second * 1,
					ServiceShutdownCtx: context.Background(),
				}

				err := validateConfig(cfg)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(ErrMissingTSStoragePath.Error()))
			})

			It("passes validation", func() {
				cfg := &Config{
					FlushInterval:      time.Second * 1,
					ServiceShutdownCtx: context.Background(),
					TSStoragePath:      "./tsdata",
				}

				err := validateConfig(cfg)
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("genCounterID", func() {
			It("returns the correct ID", func() {
				id := uuid.NewV4().String()

				want := "TYPE_SCHEMA_VIOLATION-RESOURCE_READ-" + id
				got := genCounterID(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, id)
				Expect(got).To(Equal(want))
			})
		})

		Context("AddCounter", func() {
			It("adds a counter", func() {
				Expect(len(s.counters)).To(Equal(0))

				c := s.AddCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, uuid.NewV4().String())

				Expect(c).To(BeAssignableToTypeOf(&Counter{}))
				Expect(len(s.counters)).To(Equal(1))
			})
		})

		Context("GetCounter", func() {
			It("returns an error when counter is not found", func() {
				got, err := s.GetCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, uuid.NewV4().String())
				Expect(got).To(BeNil())
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(ErrCounterNotFound.Error()))
			})

			It("gets a counter", func() {
				id := uuid.NewV4().String()

				want := s.AddCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, id)

				got, err := s.GetCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, id)
				Expect(err).ToNot(HaveOccurred())
				Expect(got).To(Equal(want))
			})
		})

		Context("RemoveCounter", func() {
			It("returns an error when counter does not exist", func() {
				err := s.RemoveCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, uuid.NewV4().String())
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to delete counter"))
			})

			It("removes a counter", func() {
				id := uuid.NewV4().String()

				s.AddCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, id)

				Expect(len(s.counters)).To(Equal(1))

				err := s.RemoveCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, id)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(s.counters)).To(Equal(0))
			})
		})

		Context("runWatchForShutdown", func() {
			It("removes counters successfully", func() {
				ctx, cancelFunc = context.WithCancel(context.Background())
				s.Config.ServiceShutdownCtx = ctx

				s.AddCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, uuid.NewV4().String())
				Expect(len(s.counters)).To(Equal(1))

				cancelFunc()
				err := s.runWatchForShutdown()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(s.counters)).To(Equal(0))
			})
		})
	})

	Context("Counter", func() {
		Context("Incr", func() {
			It("increments the value", func() {
				c := s.AddCounter(opts.Counter_TYPE_SCHEMA_VIOLATION, opts.Counter_RESOURCE_READ, uuid.NewV4().String())
				Expect(c.Value()).To(Equal(float64(0)))
				c.Incr(5)
				Expect(c.Value()).To(Equal(float64(5)))
			})
		})

		Context("getStorageLabels", func() {
			id := uuid.NewV4().String()

			c := &Counter{
				cfg: &opts.Counter{
					Resource:   opts.Counter_RESOURCE_READ,
					ResourceId: id,
				},
			}

			want := []tstorage.Label{
				{Name: "ResourceType", Value: opts.Counter_RESOURCE_READ.String()},
				{Name: "ResourceID", Value: id},
			}

			Expect(c.getStorageLabels()).To(Equal(want))
		})

		Context("flush", func() {
			fakeStorage := &tools.FakeStorage{}

			c := &Counter{
				cfg: &opts.Counter{
					Resource:   opts.Counter_RESOURCE_READ,
					Type:       opts.Counter_TYPE_SCHEMA_VIOLATION,
					ResourceId: uuid.NewV4().String(),
					Value:      1,
				},
				mtx:     &sync.RWMutex{},
				storage: fakeStorage,
			}

			err := c.flush()
			Expect(err).ToNot(HaveOccurred())
			Expect(fakeStorage.InsertRowsCallCount()).To(Equal(1))
			Expect(c.Value()).To(Equal(float64(0)))

		})

		Context("GetTSHistory", func() {
			It("returns error", func() {
				fakeStorage := &tools.FakeStorage{}
				fakeStorage.SelectStub = func(string, []tstorage.Label, int64, int64) ([]*tstorage.DataPoint, error) {
					return nil, errors.New("testing")
				}

				c := &Counter{
					cfg:     &opts.Counter{},
					storage: fakeStorage,
				}

				points, err := c.GetTSHistory(0, time.Now().UTC().Unix())
				Expect(points).To(BeNil())
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal("unable to get count history: testing"))
			})
		})

		Context("GetTotal", func() {
			It("returns the correct total", func() {
				fakeStorage := &tools.FakeStorage{}
				fakeStorage.SelectStub = func(string, []tstorage.Label, int64, int64) ([]*tstorage.DataPoint, error) {
					ts := time.Now().UTC().Unix()
					return []*tstorage.DataPoint{
						{Value: 1, Timestamp: ts},
						{Value: 2, Timestamp: ts},
						{Value: 3, Timestamp: ts},
					}, nil
				}

				c := &Counter{
					cfg:     &opts.Counter{Value: 1},
					mtx:     &sync.RWMutex{},
					storage: fakeStorage,
				}

				got, err := c.GetTotal()
				Expect(err).ToNot(HaveOccurred())
				Expect(got).To(Equal(float64(7)))
			})
		})
	})
})
