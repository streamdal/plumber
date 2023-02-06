package streamdal

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Streamdal", func() {
	Context("ListCollections", func() {
		It("returns an error when empty", func() {
			b := StreamdalWithMockResponse(200, `[]`)

			output, err := b.listCollections()
			Expect(err).To(Equal(errNoCollections))
			Expect(len(output)).To(Equal(0))
		})

		It("returns error on a bad response", func() {
			b := StreamdalWithMockResponse(200, `{}`)

			output, err := b.listCollections()
			Expect(err).To(Equal(errCollectionsFailed))
			Expect(len(output)).To(Equal(0))
		})

		It("lists collections", func() {
			apiResponse := `[
			  {
				"id": "64db7559-74f8-4c35-b70d-a7fa20566994",
				"name": "Test Collection",
				"notes": "",
				"token": "2b8c459a-3481-463a-a876-155d671796f7",
				"paused": false,
				"inserted_at": "2021-01-11T18:46:47.416139Z",
				"updated_at": "2021-02-11T16:54:53.05585Z",
				"disable_archiving": false,
				"team": {
				  "id": "90dd117d-ee04-4e05-8bee-17b691843737",
				  "name": "test-1",
				  "inserted_at": "2020-12-30T22:20:31.29824Z",
				  "updated_at": "2020-12-30T22:20:31.29824Z"
				},
				"schema": {
				  "id": "4f62e26d-682d-4c28-bcd4-a5f932eed73e",
				  "name": "Generic JSON",
				  "root_type": "",
				  "type": "json",
				  "shared": "true",
				  "archived": false,
				  "inserted_at": "2020-07-23T18:04:06.490768Z",
				  "updated_at": "2020-07-23T21:02:32.552232Z"
				},
				"datalake": {
				  "id": "e802032e-12aa-4b4d-ac4e-2cc9feb513ad",
				  "type": "aws",
				  "name": "Test Lake",
				  "status": "active",
				  "status_full": "",
				  "inserted_at": "2020-12-30T22:32:32.9821Z",
				  "updated_at": "2020-12-30T22:32:32.986047Z"
				},
				"author": {
				  "id": "c1c6c497-8594-4ae8-84ca-616172a007b3",
				  "name": "Test User",
				  "email": "testuser@streamdal.com"
				},
				"plan": {
				  "id": "c4ec4d4e-63e3-4f71-94fa-80028fc9f3b9",
				  "stripe_plan_id": "prod_IlTxuV7gXfJic8",
				  "plan_attributes": {
					"num_seats": 1,
					"replay_gb": 1,
					"storage_gb": 1,
					"num_collections": 1,
					"trial_available": true,
					"trial_length_days": 14
				  },
				  "plan_usage": {
					"seats": 1,
					"collections": 0
				  },
				  "status": "active",
				  "status_reason": "account created",
				  "inserted_at": "2021-01-22T17:44:27.888542Z",
				  "updated_at": "2021-01-22T17:44:27.888542Z"
				}
			  }
			]`

			b := StreamdalWithMockResponse(200, apiResponse)

			output, err := b.listCollections()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(output)).To(Equal(1))
			Expect(output[0].ID).To(Equal("64db7559-74f8-4c35-b70d-a7fa20566994"))
			Expect(output[0].Name).To(Equal("Test Collection"))
			Expect(output[0].Token).To(Equal("2b8c459a-3481-463a-a876-155d671796f7"))
			Expect(output[0].Paused).To(BeFalse())
			Expect(output[0].SchemaName).To(Equal("Generic JSON"))
			Expect(output[0].SchemaType).To(Equal("json"))
		})
	})

	Context("getDataLakeID", func() {
		It("returns an error when no datalakes exist", func() {
			apiResponse := `[]`

			b := StreamdalWithMockResponse(200, apiResponse)

			output, err := b.getDataLakeID()
			Expect(err).To(HaveOccurred())
			Expect(output).To(Equal(""))
			Expect(err).To(Equal(errNoDataLake))
		})

		It("returns a datalake ID", func() {
			apiResponse := `[
			  {
				"id": "e802032e-12aa-4b4d-ac4e-2cc9feb513ad",
				"type": "aws",
				"name": "e2etest1",
				"team_id": "90dd117d-ee04-4e05-8bee-17b691843737",
				"status": "active",
				"status_full": "",
				"archived": false,
				"inserted_at": "2020-12-30T22:32:32.9821Z",
				"updated_at": "2020-12-30T22:32:32.986047Z"
			  }
			]`

			b := StreamdalWithMockResponse(200, apiResponse)

			output, err := b.getDataLakeID()
			Expect(err).ToNot(HaveOccurred())
			Expect(output).To(Equal("e802032e-12aa-4b4d-ac4e-2cc9feb513ad"))
		})
	})
})
