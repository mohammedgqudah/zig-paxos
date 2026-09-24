const BoundedArray = @import("BoundedArray.zig");
const Paxos = @import("Paxos.zig");

const LastAccepted = Paxos.LastAccepted;
const NodeId = Paxos.NodeId;
const ProposalNumber = Paxos.ProposalNumber;

pub fn State(comptime T: type, comptime N: usize) type {
    return struct {
        const Self = @This();

        pub const Last = LastAccepted(T);

        pub const Acceptors = BoundedArray.Bounded(NodeId, N + 1);

        pub const Learning = struct {
            /// The we're learning about
            number: ProposalNumber,
            /// The value we're learning
            value: T,
            /// Nodes who have accepted this round (i.e. sent a Learn request)
            acceptors: Acceptors,
        };

        round: ProposalNumber = .{ .node_id = 0, .counter = 0 },

        /// role: proposer
        /// the proposed value from this node
        proposed: ?T = null,
        /// The number of promises for our current proposal
        promises: Acceptors = .{},
        /// Store the highest-numbered accepted proposal that acceptors report
        /// back in their promise.
        /// "it responds to the request [...] with the highest-numbered proposal (if any) that it has accepted."
        best_accepted: ?Last = null,

        /// role: acceptor
        /// The promised proposal number
        promised: ?ProposalNumber = null,
        accepted: ?Last = null,

        /// role: learning
        /// the value we're learning
        learning: ?Learning = null,

        /// the value agreed on
        chosen: ?T = null,

        pub fn init(node_id: NodeId) Self {
            return .{ .round = .{ .node_id = node_id, .counter = 0 } };
        }
    };
}
