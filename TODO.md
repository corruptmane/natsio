- [x] Reconnect (replay subscriptions)
- [x] Switch servers upon disconnect
- [x] Unsubscribe and process _max_msgs_ messages
- [x] TLS
- [x] Proper error handling
- [x] Proper logging
- [x] Proper limits handling
- [x] Try reconnecting on initial connect failure
- [ ] JetStream
    1. - [x] Pub, Pull/Push subscription, Ack, Nak, etc.
    2. - [x] Flow control for push consumers
    3. - [x] DIRECT get message
    4. - [ ] KV storage
    5. - [ ] Object storage
    6. - [ ] Ordered push consumers
- [ ] Tests with devcontainers
    1. - [x] NATS Core
    2. - [x] Protocol parser
    3. - [x] Headers parser
    4. - [ ] Different connections
    5. - [ ] NATS JetStream (pull/push sub, kv/object storage)
- [ ] Auth (JWT, User/Pass, NKey)
- [ ] Micro
- [ ] Cluster support (cluster endpoints discovery, update server info on `INFO` operation message, etc.)
