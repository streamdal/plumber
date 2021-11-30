# Testing Thrift

1. Write thrift data
   ```bash
   plumber write kafka --topics thrifttest --input-file test-assets/thrift/test_message.bin
   ```
2. Read from topic
   ```bash
   plumber read kafka --topics thrifttest --decode-type thrift --pretty
   ```
3. See expected output:
   ```json
   {
      "1": 321,
      "2": "mark test",
      "3": {
         "1": "submessage value here"
      },
      "4": {
         "123": 456
      }
   }
   ```