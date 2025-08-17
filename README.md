A **tiny, inspectable version of GossipSub** on top of **libp2p**: multiple nodes can **publish** messages to **topics**, and the network **gossips** them efficiently (not flooding everyone), with basic resilience against spam/duplicates.

## The layers

1. **Core logic:**

   * **Topic membership & mesh:** for each topic, keep \~D peers connected. If too few, **GRAFT** (add); if too many, **PRUNE** (remove).
   * **Heartbeat:** periodic tick that rebalances meshes and sends small control messages.
   * **Gossip control:** peers don’t forward full payloads to everyone. They send short “I have msg X” (**IHAVE**). Interested peers answer with **IWANT** to pull missing payloads.
   * **De-dup cache:** remember `MsgId`s you’ve seen to drop repeats.
   * **Validation hooks:** per-topic function to accept/reject messages.
   * **(Later) Scoring:** gently prefer well-behaved peers; penalize spammy ones.

2. **Networking (libp2p):**

   * A `NetworkBehaviour` that wires our core logic to real sockets.
   * Handles connections, serializes control/data messages, and raises events to our core.

3. **CLI + Simulator:**

   * `gossimini run …` to run a real node.
   * `gossimini pub/sub …` to publish or subscribe.
   * `gossimini simulate …` (later): many in-process peers for deterministic tests.

### Testing it out
To test it out
1. Open Terminal A:
```sh
RUST_LOG=info cargo run -p cli -- run
```
2. Open Terminal B:
```sh
RUST_LOG=info cargo run -p cli -- run {IP ADDR. EG: /ip4/121.0.0.1/tcp/12345}
```