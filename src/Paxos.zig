const std = @import("std");
const testing = std.testing;

const NodeId = u16;
const Counter = u48;
const ProposalNumber = packed struct(u64) {
    const Self = @This();

    node_id: NodeId,
    counter: Counter,

    pub fn asInt(self: Self) u64 {
        return @bitCast(self);
    }
};

/// A Paxos type to reach consensus for a value of type `T`.
///
/// This is a low-level implementation of the protocol, transport is handled separately.
pub fn Paxos(comptime T: type) type {
    return struct {
        const Self = @This();

        const Prepare = struct {
            proposal_number: ProposalNumber,
        };

        const Promise = struct {
            proposal_number: ProposalNumber,
            acceptor: NodeId,
            last_accepted: ?LastAccepted,
        };

        const Accept = struct {
            proposal_number: ProposalNumber,
            value: T,
        };

        const Learn = struct {
            proposal_number: ProposalNumber,
            value: T,
        };

        const LastAccepted = struct {
            proposal_number: ProposalNumber,
            value: T,
        };

        const LearnerState = struct {
            /// The we're learning about
            number: ProposalNumber,
            /// The value we're learning
            value: T,
            /// The number of acceptors who accepted this
            acceptors: usize,
        };

        id: NodeId,
        proposal_number: ProposalNumber,
        peers: []const NodeId,

        /// the value agreed on
        value: ?T = null,

        /// role: proposer
        /// the proposed value from this node
        proposed_value: ?T = null,
        /// The number of promises for our current proposal
        promises: usize = 0,
        /// Store the highest-numbered accepted proposal that acceptors report
        /// back in their promise.
        /// "it responds to the request [...] with the highest-numbered proposal (if any) that it has accepted."
        best_accepted: ?LastAccepted = null,

        /// role: acceptor
        /// The promised proposal number
        promised_number: ?ProposalNumber = null,
        accepted: ?LastAccepted = null,

        /// role: learning
        /// the value we're learning
        learningValue: ?LearnerState = null,

        pub fn init(
            node_id: NodeId,
            peers: []const NodeId,
        ) Self {
            return .{
                .id = node_id,
                .proposal_number = .{
                    .node_id = node_id,
                    .counter = 0,
                },
                .peers = peers,
                .value = null,
            };
        }

        /// Propose a value to peers.
        /// The caller should forward the `Prepare` command.
        pub fn propose(self: *Self, value: T) Prepare {
            self.proposed_value = value;
            self.promises = 1; // self-promise
            self.proposal_number.counter += 1;
            self.best_accepted = null;
            return .{
                .proposal_number = self.proposal_number,
            };
        }

        /// role: acceptor
        /// Recieve a prepare command, and potentionally promise the proposer.
        pub fn prepare(self: *Self, cmd: Prepare) ?Promise {
            if (self.promised_number == null or cmd.proposal_number.asInt() > self.promised_number.?.asInt()) {
                self.promised_number = cmd.proposal_number;
                return .{
                    .acceptor = self.id,
                    .proposal_number = cmd.proposal_number,
                    .last_accepted = self.accepted,
                };
            } else {
                return null;
            }
        }

        pub fn accept(self: *Self, cmd: Accept) ?Learn {
            // make sure we haven't promised a higher-numbered proposal
            if (self.promised_number) |promised_number| {
                if (promised_number.asInt() > cmd.proposal_number.asInt()) {
                    return null;
                }
            }
            self.accepted = .{
                .proposal_number = cmd.proposal_number,
                .value = cmd.value,
            };
            const reply: Learn = .{
                .proposal_number = cmd.proposal_number,
                .value = cmd.value,
            };
            _ = self.learn(reply);
            return reply;
        }

        pub fn learn(self: *Self, cmd: Learn) void {
            if (self.learningValue) |*learning| {
                switch (std.math.order(cmd.proposal_number.asInt(), learning.number.asInt())) {
                    .gt => {
                        self.learningValue = .{
                            .acceptors = 1,
                            .value = cmd.value,
                            .number = cmd.proposal_number,
                        };
                    },
                    .eq => {
                        learning.acceptors += 1;
                    },
                    .lt => {},
                }
                if (learning.acceptors == self.majority()) {
                    self.value = cmd.value;
                }
            } else {
                self.learningValue = .{
                    .acceptors = 1,
                    .value = cmd.value,
                    .number = cmd.proposal_number,
                };
            }
        }

        pub fn majority(self: *const Self) usize {
            return (self.peers.len + 1) / 2 + 1;
        }

        /// role: proposer
        /// Recieve a promise for a proposal and send
        /// an `Accept` command once the majority has promised.
        pub fn promise(self: *Self, cmd: Promise) ?Accept {
            // ignore stale promises
            if (cmd.proposal_number != self.proposal_number)
                return null;
            self.promises += 1;

            // store the highest-numbered accepted proposal from
            // all responses.
            if (self.best_accepted) |best_accepted| {
                if (cmd.last_accepted) |last_accepted| {
                    if (last_accepted.proposal_number.asInt() > best_accepted.proposal_number.asInt()) {
                        self.best_accepted = last_accepted;
                    }
                }
            } else if (cmd.last_accepted) |last_accepted| {
                self.best_accepted = last_accepted;
            }

            if (self.promises == self.majority()) {
                const reply: Accept = .{
                    .proposal_number = cmd.proposal_number,
                    // either send our proposed value, or propagate the value of the highest
                    // previous round.
                    .value = if (self.best_accepted) |best_accepted|
                        best_accepted.value
                    else
                        self.proposed_value.?,
                };
                _ = self.accept(reply);
                return reply;
            } else return null;
        }
    };
}

test "it will promise to not accept proposals lower the n" {
    var proposer: Paxos(u32) = .init(0, &.{1});
    const old_prepare = proposer.propose(0xdeadbeef);
    const new_prepare = proposer.propose(42);

    var acceptor: Paxos(u32) = .init(1, &.{0});

    var maybe_promise = acceptor.prepare(new_prepare);

    try testing.expect(maybe_promise != null);
    try testing.expect(maybe_promise.?.proposal_number == proposer.proposal_number);

    maybe_promise = acceptor.prepare(old_prepare);
    try testing.expect(maybe_promise == null);
}

test "a proposer will send an accept command if it recieves promises from a majority" {
    var proposer: Paxos(u32) = .init(0, &.{ 1, 2, 3, 4 });
    const prepare = proposer.propose(0xcafe);

    var acc1: Paxos(u32) = .init(1, &.{ 0, 2, 3, 4 });
    var acc2: Paxos(u32) = .init(2, &.{ 0, 1, 3, 4 });
    var acc3: Paxos(u32) = .init(3, &.{ 0, 1, 2, 4 });
    var acc4: Paxos(u32) = .init(4, &.{ 0, 1, 2, 3 });

    var accept: ?Paxos(u32).Accept = undefined;

    accept = proposer.promise(acc1.prepare(prepare).?);
    try testing.expect(accept == null);

    // third promise (including self) -> majority
    accept = proposer.promise(acc2.prepare(prepare).?);
    try testing.expect(accept != null);

    accept = proposer.promise(acc3.prepare(prepare).?);
    try testing.expect(accept == null);

    accept = proposer.promise(acc4.prepare(prepare).?);
    try testing.expect(accept == null);
}

test "a proposer will send an accept command with the value of the highest-numbered accepted proposal" {
    var proposer: Paxos(u32) = .init(0, &.{ 1, 2, 3, 4 });
    const old_prepare = proposer.propose(0xcafe);

    var acc1: Paxos(u32) = .init(1, &.{ 0, 2, 3, 4 });
    var acc2: Paxos(u32) = .init(2, &.{ 0, 1, 3, 4 });
    var acc3: Paxos(u32) = .init(3, &.{ 0, 1, 2, 4 });
    var acc4: Paxos(u32) = .init(4, &.{ 0, 1, 2, 3 });

    _ = proposer.promise(acc1.prepare(old_prepare).?);
    var accept = proposer.promise(acc2.prepare(old_prepare).?).?;
    try testing.expectEqual(0xcafe, accept.value);

    _ = acc1.accept(accept); // acc1 accepted 0xcafe
    // assume accept messages for the rest of the nodes were dropped

    // acc4 will act as a proposer now
    var new_prepare = acc4.propose(0xdead);

    _ = acc4.promise(acc3.prepare(new_prepare).?);
    accept = acc4.promise(acc2.prepare(new_prepare).?).?;
    try testing.expectEqual(0xdead, accept.value);
    _ = acc2.accept(accept);

    // state
    // acc1 accepted = 0xcafe
    // acc2 accepted = 0xdead

    // acc3 will act as a proposer now
    acc3.proposal_number.counter += 10; // make it the highest so far
    new_prepare = acc3.propose(0x6767);
    _ = acc3.promise(acc1.prepare(new_prepare).?);
    accept = acc3.promise(acc2.prepare(new_prepare).?).?;

    // the accepted was from the highest numbered proposal so far
    try testing.expectEqual(0xdead, accept.value);
}

test "a value is chosen once it is learned from the majority" {
    var learner: Paxos(u32) = .init(0, &.{ 1, 2, 3, 4 });
    var proposer: Paxos(u32) = .init(1, &.{ 0, 2, 3, 4 });
    var acc2: Paxos(u32) = .init(2, &.{ 0, 1, 3, 4 });
    var acc3: Paxos(u32) = .init(3, &.{ 0, 1, 2, 4 });
    var acc4: Paxos(u32) = .init(4, &.{ 0, 1, 2, 3 });

    const prepare = proposer.propose(42);
    _ = proposer.promise(acc2.prepare(prepare).?);
    const accept = proposer.promise(acc3.prepare(prepare).?).?;
    
    learner.learn(acc2.accept(accept).?);
    try testing.expectEqual(null, learner.value);
    learner.learn(acc3.accept(accept).?);
    try testing.expectEqual(null, learner.value);

    learner.learn(acc4.accept(accept).?);
    try testing.expectEqual(42, learner.value);
}
