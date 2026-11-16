# Raft KV store

A Java 17 and Spring Boot key/value service with Raft leader election, log replication, majority commits, gRPC node RPCs, Protocol Buffers messages, virtual hash partitions, and persistent Raft state for restart recovery.

## Package layout

```text
dev.example.raftkv
├── config/          Spring and Raft configuration
├── constants/       Shared API paths and Raft operation names
├── controller/      HTTP endpoints
├── dto/
│   ├── kv/          Key/value API request and error payloads
│   ├── order/       Order and idempotency request/response models
│   └── raft/        Internal Raft RPC payloads
├── enums/           Raft node roles
├── exception/       Domain exceptions and centralized HTTP error handling
├── model/           Raft log entry model
├── repository/      Durable Raft state loading and persistence
├── service/         Consensus contract/implementation and order workflow
└── transport/       Peer RPC contract
    └── grpc/        gRPC peer client implementation and server adapter
```

The controller and gRPC server depend on the `RaftConsensusService` interface. The consensus implementation depends on `RaftStateRepository` and `RaftPeerClient` contracts; Spring wires `JsonFileRaftStateRepository` and `GrpcRaftPeerClient` as their current implementations. This keeps the consensus logic independent of file storage and gRPC details, so those adapters can be replaced without changing the API layer or Raft algorithm.

## Run the Docker cluster

```sh
docker compose up --build
```

The three nodes expose HTTP APIs on ports 8081, 8082, and 8083. Raft gRPC traffic uses port 9091 inside the Docker network. Each node has a separate persistent Docker volume under `/data`. The Compose `RAFT_HTTP_PEERS` values use these host ports so redirects returned to Postman resolve from the host machine.

## Run locally

Build using Maven (`mvn package`) and start three processes. Peer targets use `node-id=host:grpc-port`:

```sh
RAFT_NODE_ID=node1 SERVER_PORT=8081 RAFT_GRPC_PORT=9091 RAFT_DATA_DIR=./data RAFT_PEERS='node2=localhost:9092,node3=localhost:9093' RAFT_HTTP_PEERS='node2=http://localhost:8082,node3=http://localhost:8083' java -jar target/raft-kv-0.1.0.jar
RAFT_NODE_ID=node2 SERVER_PORT=8082 RAFT_GRPC_PORT=9092 RAFT_DATA_DIR=./data RAFT_PEERS='node1=localhost:9091,node3=localhost:9093' RAFT_HTTP_PEERS='node1=http://localhost:8081,node3=http://localhost:8083' java -jar target/raft-kv-0.1.0.jar
RAFT_NODE_ID=node3 SERVER_PORT=8083 RAFT_GRPC_PORT=9093 RAFT_DATA_DIR=./data RAFT_PEERS='node1=localhost:9091,node2=localhost:9092' RAFT_HTTP_PEERS='node1=http://localhost:8081,node2=http://localhost:8082' java -jar target/raft-kv-0.1.0.jar
```

Follower nodes redirect order reads (`GET /v1/orders/{orderId}` and `GET /v1/orders/requests/{idempotencyKey}`) to the known leader so the read remains linearizable. `RAFT_HTTP_PEERS` maps each other node ID to its HTTP base URL. In Postman, enable **Automatically follow redirects** to receive the leader's response while sending the request to any node.

## API

```sh
curl -i -X PUT localhost:8081/v1/kv/color -H 'Content-Type: application/json' -d '{"value":"blue"}'
curl localhost:8081/v1/kv/color
curl -i -X DELETE localhost:8081/v1/kv/color
curl localhost:8081/v1/raft/status
```

### E-commerce order idempotency demo

`POST /v1/orders` creates an order using a client-supplied `Idempotency-Key`. The key is scoped to `customerId`. The Raft state machine atomically claims a new key, rejects reuse with a different normalized order body, stores the order under a stable order ID, and completes the idempotency record with compare-and-set. If a node stops between steps, retrying the same key resumes the same order. Completed records can be reused after seven days by default; change this with `IDEMPOTENCY_RETENTION_HOURS`. Expiration is lazy: an expired key is replaced when that key is used again, while background removal of unused expired records is not implemented.

Send this request to the current leader (replace port `8081` with the leader's HTTP port if needed):

```http
POST http://localhost:8081/v1/orders
Content-Type: application/json
Idempotency-Key: checkout-2026-0001

{
  "customerId": "buyer-42",
  "items": [
    { "sku": "SKU-RED-42", "quantity": 2 },
    { "sku": "SKU-BLUE-10", "quantity": 1 }
  ]
}
```

The first successful request returns `201 Created` with an order ID and `replayed: false`. Repeat the same request with the same key: it returns `200 OK`, the same order ID, and `replayed: true`. Reuse the key with different items and the API returns `409 Conflict` with code `IDEMPOTENCY_KEY_REUSED`.

Check the saved order and request record with `GET /v1/orders/{orderId}` and `GET /v1/orders/requests/{idempotencyKey}?customerId=buyer-42`. These reads use a committed Raft read barrier. The demo stores order and idempotency JSON in Raft KV; it does not yet integrate a relational order database, inventory reservation, or a payment provider.

Writes sent to a follower return `503` with its known leader ID. Successful writes return only after a majority acknowledges the log entry and it is applied to the state machine. If quorum is unavailable until `RAFT_WRITE_COMMIT_TIMEOUT_MS` elapses, the API returns `504` with code `RAFT_WRITE_COMMIT_TIMEOUT`; the entry may still commit later, so clients should read before retrying non-idempotent operations. Reads report the deterministic virtual partition selected by hashing the key. The current Raft group replicates all partitions to all members; partition buckets organize the state but do not yet provide independent shard groups or horizontal data placement.

The protobuf service definition is in `src/main/proto/raft.proto`. Nodes communicate through its `RequestVote` and `AppendEntries` methods. Each node saves its term, vote, log, and commit index to an atomic JSON state file under `<RAFT_DATA_DIR>/<RAFT_NODE_ID>/raft-state.json`, then reconstructs committed key/value state at startup.

HTTP errors use a stable `code` field alongside the human-readable `error` and optional `leaderId`. Current codes include `RAFT_NOT_LEADER`, `RAFT_WRITE_COMMIT_TIMEOUT`, `IDEMPOTENCY_KEY_REUSED`, `INVALID_ORDER`, `VALIDATION_ERROR`, `INVALID_REQUEST`, and `INTERNAL_ERROR`.

Committed state is snapshotted after `RAFT_SNAPSHOT_THRESHOLD` newly applied log entries (default `100`). Each snapshot records the last included index and term plus the partition state; the covered log prefix is compacted. Lagging peers receive the snapshot over the protobuf `InstallSnapshot` RPC before normal log replication resumes. Term, vote, snapshot, remaining log, and commit index are persisted together for restart recovery.

Run the consensus, quorum failure, and snapshot recovery tests with `mvn test`.

This order slice stores order JSON and idempotency records in the same Raft replicated state machine. It demonstrates request de-duplication and crash-resumable order creation, but does not integrate PostgreSQL, inventory reservations, payment processing, or authentication. In a real commerce service, derive `customerId` from authenticated identity, keep order details in a transactional database with a unique idempotency constraint, and use stable idempotency IDs with inventory/payment services. The overall project is for learning/development: it does not include dynamic Raft membership, authenticated/encrypted peer traffic, or independent Raft groups per partition. Configure reliable persistent volumes and protect gRPC ports before exposing a cluster beyond a trusted network.
