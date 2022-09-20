namespace go sh.batch.schema

include "inctest2.thrift"

struct SubMessage {
    1: string value
}

enum ClientType {
  UNSET = 0,
  NORMAL = 1,
  VIP = 2
}

union Thing {
  1: string thing_string
  2: i32 thing_int
}

const i32 INT_CONST = 1234;

typedef double USD

struct Account {
  1: i32 id
  2: string name
  3: SubMessage subm
  4: map<i32, string> teams
  5: list<string> emails
  6: ClientType type = ClientType.NORMAL
  7: inctest2.IncludedMessage model
  8: Thing unionthing
  9: USD price
  10: i32 testconst = INT_CONST
  11: set<string> permissions
}