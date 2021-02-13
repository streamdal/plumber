package batch

import (
	"testing"

	. "github.com/onsi/gomega"
)

func TestListCollections_empty(t *testing.T) {
	g := NewGomegaWithT(t)

	b := BatchWithMockResponse(200, `[]`)

	output, err := b.listCollections()
	g.Expect(err).To(Equal(errNoCollections))
	g.Expect(len(output)).To(Equal(0))
}

func TestListCollections_bad_response(t *testing.T) {
	g := NewGomegaWithT(t)

	b := BatchWithMockResponse(200, `{}`)

	output, err := b.listCollections()
	g.Expect(err).To(Equal(errCollectionsFailed))
	g.Expect(len(output)).To(Equal(0))
}

func TestListCollections(t *testing.T) {
	g := NewGomegaWithT(t)

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
      "email": "testuser@batch.sh"
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

	b := BatchWithMockResponse(200, apiResponse)

	output, err := b.listCollections()
	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(len(output)).To(Equal(1))
	g.Expect(output[0].ID).To(Equal("64db7559-74f8-4c35-b70d-a7fa20566994"))
	g.Expect(output[0].Name).To(Equal("Test Collection"))
	g.Expect(output[0].Token).To(Equal("2b8c459a-3481-463a-a876-155d671796f7"))
	g.Expect(output[0].Paused).To(BeFalse())
	g.Expect(output[0].SchemaName).To(Equal("Generic JSON"))
	g.Expect(output[0].SchemaType).To(Equal("json"))
}
