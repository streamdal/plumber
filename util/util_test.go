package util

import (
	"crypto/tls"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Utility Package", func() {
	Context("GenerateTLSConfig", func() {

		var tlsSkipVerify bool
		var tlsCaCert, tlsClientCert, tlsClientKey string

		BeforeEach(func() {
			tlsSkipVerify = false
			tlsCaCert = "../test-assets/ssl/ca.crt"
			tlsClientCert = "../test-assets/ssl/client.crt"
			tlsClientKey = "../test-assets/ssl/client.key"
		})

		It("works with files", func() {
			tlsConfig, err := GenerateTLSConfig(tlsCaCert, tlsClientCert, tlsClientKey, tlsSkipVerify, tls.NoClientCert)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(tlsConfig.Certificates)).To(Equal(1))
		})
		It("returns error on incorrect cert file", func() {
			// Cert and key arguments are swapped
			_, err := GenerateTLSConfig(tlsCaCert, tlsClientKey, tlsClientCert, tlsSkipVerify, tls.NoClientCert)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to find certificate PEM data in certificate input"))
		})
		It("returns error on incorrect cert string", func() {
			caBytes, err := ioutil.ReadFile("../test-assets/ssl/ca.crt")
			Expect(err).ToNot(HaveOccurred())
			certBytes, err := ioutil.ReadFile("../test-assets/ssl/client.crt")
			Expect(err).ToNot(HaveOccurred())
			keyBytes, err := ioutil.ReadFile("../test-assets/ssl/client.key")
			Expect(err).ToNot(HaveOccurred())

			// Cert and key arguments are swapped
			_, err = GenerateTLSConfig(string(caBytes), string(keyBytes), string(certBytes), tlsSkipVerify, tls.NoClientCert)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to find certificate PEM data in certificate input"))
		})

		It("works with strings", func() {
			caBytes, err := ioutil.ReadFile("../test-assets/ssl/ca.crt")
			Expect(err).ToNot(HaveOccurred())
			certBytes, err := ioutil.ReadFile("../test-assets/ssl/client.crt")
			Expect(err).ToNot(HaveOccurred())
			keyBytes, err := ioutil.ReadFile("../test-assets/ssl/client.key")
			Expect(err).ToNot(HaveOccurred())

			tlsCaCert = string(caBytes)
			tlsClientCert = string(certBytes)
			tlsClientKey = string(keyBytes)
			tlsSkipVerify = true

			tlsConfig, err := GenerateTLSConfig(tlsCaCert, tlsClientCert, tlsClientKey, tlsSkipVerify, tls.NoClientCert)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(tlsConfig.Certificates)).To(Equal(1))
		})
	})
})
