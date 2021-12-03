# Mongo test setup

1. Bring up mongo container `docker-compose up -d`
2. Exec into container `docker exec -it mongodb /bin/bash`
3. Enter mongo shell `mongo`
   1. Run the following
      ```
      rs.initiate({
         _id: "rs0",
         version: 1,
         members: [
            { _id: 0, host : "localhost:27017" }
         ]
      })
      ```
   2. Create database `use plumbertest`
6. Start plumber: `plumber read mongo --database plumbertest --follow`
7. Back in `mongo` console, insert some data
   ```
   db.plumbertest.insert({name:"Mark",company:"Batch.sh"})
   ```
8. Plumber should output change set
   ```json
   {
      "_id": {
        "_data": "826078C08F000000012B022C0100296E5A10046786BB805FD94CADB30440A98672B49246645F696400646078C08FD6D1C97307EF02530004"
      },
         "clusterTime": {
         "$timestamp": {
         "i": "1",
         "t": "1618526351"
      }
      },
         "documentKey": {
         "_id": {
         "$oid": "6078c08fd6d1c97307ef0253"
      }
      },
      "fullDocument": {
         "_id": {
          "$oid": "6078c08fd6d1c97307ef0253"
         },
         "company": "Batch.sh",
         "name": "Mark"
      },
      "ns": {
         "coll": "plumbertest",
         "db": "plumbertest"
      },
      "operationType": "insert"
   }
   ```