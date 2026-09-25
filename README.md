# Paxos
This is an implementation of the single-decree [Paxos](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) distributed consensus algorithm.

# Usage
The algorithm itself is written in [`Paxos.zig`](https://github.com/mohammedgqudah/zig-paxos/blob/main/src/Paxos.zig) which is only a state machine, it does not handle IO (transport, persistence). Each step produces a new state and an optional message, which a "driver" should save and broadcast.

```zig
// consensus for type `T`, for a 3-node cluster.
var node: Paxos(T, 3) = .init(node_id, &.{ peer1, peer2 });

const proposal = node.propose(proposed_value);

try save(proposal.state);
node.state = proposal.state;

broadcast(peers, .{ .prepare = proposal.prepare });

// handle incoming messages
for (incoming) |message| {
    const result = node.apply(message);

    try save(result.state);
    node.state = result.state;

    if (result.message) |reply|
        broadcast(peers, reply);
}
```
