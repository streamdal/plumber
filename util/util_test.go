package util

import (
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Utility Package", func() {
	Context("GenerateTLSConfig", func() {

		var tlsSkipVerify bool
		var tlsCaCert, tlsClientCert, tlsClientKey []byte

		BeforeEach(func() {
			tlsSkipVerify = false
			tlsCaCert = []byte(`../test-assets/ssl/ca.crt`)
			tlsClientCert = []byte(`../test-assets/ssl/client.crt`)
			tlsClientKey = []byte(`../test-assets/ssl/client.key`)
		})

		It("works with files", func() {
			tlsConfig, err := GenerateTLSConfig(tlsCaCert, tlsClientCert, tlsClientKey, tlsSkipVerify)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(tlsConfig.Certificates)).To(Equal(1))
		})
		It("returns error on incorrect cert file", func() {
			// Cert and key arguments are swapped
			_, err := GenerateTLSConfig(tlsCaCert, tlsClientKey, tlsClientCert, tlsSkipVerify)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to load client certificate: tls: failed to find certificate PEM data in certificate input, but did find a private key; PEM inputs may have been switched"))
		})
		It("returns error on incorrect cert string", func() {
			caBytes, err := ioutil.ReadFile("../test-assets/ssl/ca.crt")
			Expect(err).ToNot(HaveOccurred())
			certBytes, err := ioutil.ReadFile("../test-assets/ssl/client.crt")
			Expect(err).ToNot(HaveOccurred())
			keyBytes, err := ioutil.ReadFile("../test-assets/ssl/client.key")
			Expect(err).ToNot(HaveOccurred())

			_, err = GenerateTLSConfig(caBytes, certBytes, keyBytes, tlsSkipVerify)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unable to load ssl keypair"))
		})

		It("works with strings", func() {
			caBytes, err := ioutil.ReadFile("../test-assets/ssl/ca.crt")
			Expect(err).ToNot(HaveOccurred())
			certBytes, err := ioutil.ReadFile("../test-assets/ssl/client.crt")
			Expect(err).ToNot(HaveOccurred())
			keyBytes, err := ioutil.ReadFile("../test-assets/ssl/client.key")
			Expect(err).ToNot(HaveOccurred())

			tlsCaCert = caBytes
			tlsClientCert = certBytes
			tlsClientKey = keyBytes
			tlsSkipVerify = true

			tlsConfig, err := GenerateTLSConfig(tlsCaCert, tlsClientCert, tlsClientKey, tlsSkipVerify)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(tlsConfig.Certificates)).To(Equal(1))
		})
	})
})
