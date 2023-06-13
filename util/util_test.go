package util

import (
	"crypto/tls"
	"encoding/base64"
	"io/ioutil"
	"math/rand"
	"time"

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

	Context("IsBase64", func() {
		It("returns correct value", func() {
			// Some static cases
			cases := map[string]bool{
				"aGVsbG8gd29ybGQ=": true,
				"sample string":    false,
				"a":                false,
				"1234":             false,
			}

			// Ten thousand base64 encoded random bytes
			for i := 0; i < 10000; i++ {
				randBytes := make([]byte, 32)
				rand.Seed(time.Now().UnixNano())
				rand.Read(randBytes)
				randString := base64.StdEncoding.EncodeToString(randBytes)
				cases[randString] = true
			}

			for v, want := range cases {
				Expect(IsBase64(v)).To(Equal(want))
			}
		})
	})

	Context("compareVersions", func() {
		It("returns true for patch version increase", func() {
			Expect(CompareVersions("v1.0.0", "v1.0.1")).To(BeTrue())
		})
		It("returns false for patch version decrease and major version increase", func() {
			Expect(CompareVersions("v2.0.0", "v1.0.1")).To(BeFalse())
		})

		It("returns true for minor version increase", func() {
			Expect(CompareVersions("v1.0.0", "v1.1.0")).To(BeTrue())
		})
		It("returns false for minor version decrease", func() {
			Expect(CompareVersions("v1.0.1", "v1.0.0")).To(BeFalse())
		})

		It("returns true for major version increase", func() {
			Expect(CompareVersions("v1.9.9", "v2.0.0")).To(BeTrue())
		})
		It("returns false for major version decrease", func() {
			Expect(CompareVersions("v2.0.0", "v1.9.9")).To(BeFalse())
		})
	})
})
