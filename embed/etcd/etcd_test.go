package etcd

//
//import (
//	"context"
//	"fmt"
//	"net/url"
//	"time"
//
//	"github.com/batchcorp/plumber/config"
//
//	. "github.com/onsi/ginkgo"
//	. "github.com/onsi/gomega"
//	"github.com/pkg/errors"
//	uuid "github.com/satori/go.uuid"
//	clientv3 "go.etcd.io/etcd/client/v3"
//)
//
//var _ = Describe("Etcd", func() {
//	var (
//		pURL, _ = url.Parse("http://127.0.0.1:2380")
//		cURL, _ = url.Parse("http://127.0.0.1:2379")
//
//		goodServerOptions = &cli.ServerOptions{
//			NodeID:             "plumber1",
//			ListenAddress:      "127.0.0.1:9000", // grpc address
//			AuthToken:          "secret",
//			InitialCluster:     "plumber1=http://127.0.0.1:2380",
//			AdvertisePeerURL:   pURL,
//			AdvertiseClientURL: cURL,
//			ListenerPeerURL:    pURL,
//			ListenerClientURL:  cURL,
//			PeerToken:          "secret",
//		}
//
//		serviceContext, _ = context.WithCancel(context.Background())
//		srv               *Etcd
//	)
//
//	// Go run tests in parallel and because launching 20+ etcd's will probably
//	// be heavy - we launch a single etcd for the entire test. The BeforeSuite
//	// and AfterSuite test that start and shutdown don't error.
//	BeforeSuite(func() {
//		var newErr error
//
//		srv, newErr = New(goodServerOptions, &config.Config{})
//		Expect(newErr).ToNot(HaveOccurred())
//		Expect(srv).ToNot(BeNil())
//		Expect(srv.started).To(BeFalse())
//
//		startErr := srv.Start(serviceContext)
//		Expect(startErr).ToNot(HaveOccurred())
//		Expect(srv.started).To(BeTrue())
//	})
//
//	AfterSuite(func() {
//		shutdownErr := srv.Shutdown(true)
//		Expect(shutdownErr).ToNot(HaveOccurred())
//
//		writeReadErr := writeRead(goodServerOptions, uuid.NewV4().String(), uuid.NewV4().String())
//		Expect(writeReadErr).To(HaveOccurred())
//		Expect(writeReadErr.Error()).To(ContainSubstring("unable to PUT"))
//	})
//
//	// We know that Start and Shutdown works so we test other by-products
//	Context("Start", func() {
//		It("launches consumers", func() {
//			// Write something to /bus/broadcast/* and /bus/queue/$nodeid/*
//		})
//	})
//
//	Context("Broadcast", func() {
//		It("writes to the correct path", func() {
//			// check etcd
//		})
//
//		It("fires broadcast handler", func() {
//
//		})
//
//		It("errors with bad args", func() {
//
//		})
//	})
//
//	Context("Direct", func() {
//		It("writes to the correct path", func() {
//			// Check etcd
//		})
//
//		It("fires broadcast handler", func() {
//
//		})
//
//		It("errors with bad args", func() {
//
//		})
//	})
//})
//
//// wrapper func for writing something to etcd and reading it back
//func writeRead(opts *cli.ServerOptions, key, val string) error {
//	// Put  data
//	c, err := clientv3.New(clientv3.Config{
//		Endpoints: []string{opts.ListenerClientURL.String()},
//	})
//
//	if err != nil {
//		return errors.Wrap(err, "unable to create etcd client")
//	}
//
//	ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
//
//	if _, err := c.Put(ctxWithTimeout, key, val); err != nil {
//		return errors.Wrap(err, "unable to PUT")
//	}
//
//	getResp, err := c.Get(ctxWithTimeout, key)
//	if err != nil {
//		return errors.Wrap(err, "unable to GET")
//	}
//
//	if getResp == nil {
//		return errors.New("getResp is nil")
//	}
//
//	if len(getResp.Kvs) != 1 {
//		return errors.New("received invalid number of Kvs")
//	}
//
//	if string(getResp.Kvs[0].Value) != val {
//		return fmt.Errorf("getresp value does not match: got '%s', expected '%s'",
//			string(getResp.Kvs[0].Value), val)
//	}
//
//	return nil
//}
