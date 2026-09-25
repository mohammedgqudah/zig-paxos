# Paxos
This is an implementation of the single-decree [Paxos](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) distributed consensus algorithm.

# Usage
The algorithm itself is written in [`Paxos.zig`](https://github.com/mohammedgqudah/zig-paxos/blob/main/src/Paxos.zig) which is only a state machine, it does not handle IO (transport, persistence). Each step produces a new state and a reply, which a "driver" should save and broadcast.

```zig
// consensus for type `T`, for a 3-node cluster.
var node: Paxos(T, 3) = .init(node_id, &.{ peer1, peer2 });

const result = node.propose(proposed_value);
try save(result.state);
node.state = result.state;

// broadcast message to peers
for (peers) |peer| {
   peer.send(result.prepare);
}

// handle incoming requests
for (incoming_messages) |message| {
    switch (message.type) {
        .promise => {
            const result = node.promise(message.promise);
            try save(result.state);
            node.state = result.state;
        },
        else => {}
    }
}
```
