const std = @import("std");
const State = @import("State.zig").State;
const testing = std.testing;

pub const NodeId = u16;
pub const Counter = u48;
pub const ProposalNumber = packed struct(u64) {
    const Self = @This();

    node_id: NodeId,
    counter: Counter,

    pub fn asInt(self: Self) u64 {
        return @bitCast(self);
    }
};

pub fn LastAccepted(comptime T: type) type {
    return struct {
        proposal_number: ProposalNumber,
        value: T,
    };
}

/// A Paxos type to reach consensus for a value of type `T`.
///
/// This is a low-level implementation of the protocol, transport is handled separately.
pub fn Paxos(comptime T: type, comptime N: usize) type {
    return struct {
        const Self = @This();

        pub const PaxosState = State(T, N);

        pub const Prepare = struct {
            proposal_number: ProposalNumber,
        };

        pub const Promise = struct {
            proposal_number: ProposalNumber,
            acceptor: NodeId,
            last_accepted: ?LastAccepted(T),
        };

        pub const Accept = struct {
            proposal_number: ProposalNumber,
            value: T,
        };

        pub const Learn = struct {
            proposal_number: ProposalNumber,
            value: T,
            acceptor: NodeId,
        };

        pub const ProposeResult = struct { state: PaxosState, prepare: Prepare };
        pub const PrepareResult = struct { state: PaxosState, promise: ?Promise };
        pub const PromiseResult = struct { state: PaxosState, accept: ?Accept };
        pub const AcceptResult = struct { state: PaxosState, learn: ?Learn };

        id: NodeId,
        peers: []const NodeId,
        state: PaxosState,

        pub fn init(node_id: NodeId, peers: []const NodeId) Self {
            return .{
                .id = node_id,
                .peers = peers,
                .state = PaxosState.init(node_id),
            };
        }

        fn withState(self: *const Self, s: PaxosState) Self {
            return .{ .id = self.id, .peers = self.peers, .state = s };
        }

        /// Propose a value to peers.
        /// The caller should forward the `Prepare` command.
        pub fn propose(self: *const Self, value: T) ProposeResult {
            var node = self.withState(self.state);
            node.state.proposed = value;
            node.state.promises = .{};
            node.state.round.counter += 1;
            const prepare_cmd: Prepare = .{ .proposal_number = node.state.round };

            // self-promise, but only if we haven't promised a higher numbered round.
            const prepared = node.prepare(prepare_cmd);
            node.state = prepared.state;
            if (prepared.promise) |self_promise| {
                node.state = node.promise(self_promise).state;
            }
            return .{ .state = node.state, .prepare = prepare_cmd };
        }

        /// role: acceptor
        /// Receive a prepare command, and potentially promise the proposer.
        pub fn prepare(self: *const Self, cmd: Prepare) PrepareResult {
            var s = self.state;
            if (s.promised == null or cmd.proposal_number.asInt() > s.promised.?.asInt()) {
                s.promised = cmd.proposal_number;
                return .{
                    .state = s,
                    .promise = .{
                        .proposal_number = cmd.proposal_number,
                        .acceptor = self.id,
                        .last_accepted = s.accepted,
                    },
                };
            }
            return .{ .state = s, .promise = null };
        }

        /// role: proposer
        /// Receive a promise for a proposal and send
        /// an `Accept` command once the majority has promised.
        pub fn promise(self: *const Self, cmd: Promise) PromiseResult {
            var s = self.state;

            // ignore stale promises
            if (cmd.proposal_number != s.round) return .{ .state = s, .accept = null };

            // ignore duplicate messages
            if (s.promises.contains(cmd.acceptor)) return .{ .state = s, .accept = null };
            s.promises.append(cmd.acceptor) catch return .{ .state = s, .accept = null };

            // store the highest-numbered accepted proposal from
            // all responses.
            if (cmd.last_accepted) |last_accepted| {
                if (s.best_accepted) |best| {
                    if (last_accepted.proposal_number.asInt() > best.proposal_number.asInt()) {
                        s.best_accepted = last_accepted;
                    }
                } else {
                    s.best_accepted = last_accepted;
                }
            }

            if (s.promises.len != self.majority()) return .{ .state = s, .accept = null };

            const accept_cmd: Accept = .{
                .proposal_number = cmd.proposal_number,
                // either send our proposed value, or propagate the value of the highest
                // previous round.
                .value = if (s.best_accepted) |best| best.value else s.proposed.?,
            };
            var node = self.withState(s);
            node.state = node.accept(accept_cmd).state;
            return .{ .state = node.state, .accept = accept_cmd };
        }

        pub fn accept(self: *const Self, cmd: Accept) AcceptResult {
            var s = self.state;

            // make sure we haven't promised a higher-numbered proposal
            if (s.promised) |promised| {
                if (promised.asInt() > cmd.proposal_number.asInt()) {
                    return .{ .state = s, .learn = null };
                }
            }

            s.accepted = .{ .proposal_number = cmd.proposal_number, .value = cmd.value };
            const learn_cmd: Learn = .{
                .proposal_number = cmd.proposal_number,
                .value = cmd.value,
                .acceptor = self.id,
            };
            var node = self.withState(s);
            node.state = node.learn(learn_cmd);
            return .{ .state = node.state, .learn = learn_cmd };
        }

        pub fn learn(self: *const Self, cmd: Learn) PaxosState {
            var s = self.state;

            if (s.learning) |*learning| {
                // ignore duplicate
                if (learning.acceptors.contains(cmd.acceptor)) return s;
                switch (std.math.order(cmd.proposal_number.asInt(), learning.number.asInt())) {
                    .gt => s.learning = .{
                        .number = cmd.proposal_number,
                        .value = cmd.value,
                        .acceptors = PaxosState.Acceptors.initOne(cmd.acceptor),
                    },
                    .eq => learning.acceptors.append(cmd.acceptor) catch {},
                    .lt => return s,
                }
            } else {
                s.learning = .{
                    .number = cmd.proposal_number,
                    .value = cmd.value,
                    .acceptors = PaxosState.Acceptors.initOne(cmd.acceptor),
                };
            }

            if (s.learning.?.acceptors.len == self.majority()) {
                s.chosen = cmd.value;
            }
            return s;
        }

        pub fn majority(self: *const Self) usize {
            return (self.peers.len + 1) / 2 + 1;
        }
    };
}

test "it will promise to not accept proposals lower the n" {
    var proposer: Paxos(u32, 3) = .init(0, &.{ 1, 2 });
    const older = proposer.propose(0xdeadbeef);
    proposer.state = older.state;
    const newer = proposer.propose(42);
    proposer.state = newer.state;

    var acceptor: Paxos(u32, 3) = .init(1, &.{ 0, 2 });

    var result = acceptor.prepare(newer.prepare);
    acceptor.state = result.state;
    try testing.expect(result.promise != null);
    try testing.expect(result.promise.?.proposal_number == proposer.state.round);

    result = acceptor.prepare(older.prepare);
    acceptor.state = result.state;
    try testing.expect(result.promise == null);
}

test "a proposer will send an accept command if it receives promises from a majority" {
    var proposer: Paxos(u32, 5) = .init(0, &.{ 1, 2, 3, 4 });
    const proposal = proposer.propose(0xcafe);
    proposer.state = proposal.state;

    var acc1: Paxos(u32, 5) = .init(1, &.{ 0, 2, 3, 4 });
    var acc2: Paxos(u32, 5) = .init(2, &.{ 0, 1, 3, 4 });
    var acc3: Paxos(u32, 5) = .init(3, &.{ 0, 1, 2, 4 });
    var acc4: Paxos(u32, 5) = .init(4, &.{ 0, 1, 2, 3 });

    const p1 = acc1.prepare(proposal.prepare);
    acc1.state = p1.state;
    var result = proposer.promise(p1.promise.?);
    proposer.state = result.state;
    try testing.expect(result.accept == null);

    // send acc1_promise again to ensure that we
    // tolerate duplicate promises.
    result = proposer.promise(p1.promise.?);
    proposer.state = result.state;
    try testing.expect(result.accept == null);

    // third promise (including self) -> majority
    const p2 = acc2.prepare(proposal.prepare);
    acc2.state = p2.state;
    result = proposer.promise(p2.promise.?);
    proposer.state = result.state;
    try testing.expect(result.accept != null);

    const p3 = acc3.prepare(proposal.prepare);
    acc3.state = p3.state;
    result = proposer.promise(p3.promise.?);
    proposer.state = result.state;
    try testing.expect(result.accept == null);

    const p4 = acc4.prepare(proposal.prepare);
    acc4.state = p4.state;
    result = proposer.promise(p4.promise.?);
    proposer.state = result.state;
    try testing.expect(result.accept == null);
}

test "a proposer will send an accept command with the value of the highest-numbered accepted proposal" {
    var proposer: Paxos(u32, 5) = .init(0, &.{ 1, 2, 3, 4 });
    const old = proposer.propose(0xcafe);
    proposer.state = old.state;

    var acc1: Paxos(u32, 5) = .init(1, &.{ 0, 2, 3, 4 });
    var acc2: Paxos(u32, 5) = .init(2, &.{ 0, 1, 3, 4 });
    var acc3: Paxos(u32, 5) = .init(3, &.{ 0, 1, 2, 4 });
    var acc4: Paxos(u32, 5) = .init(4, &.{ 0, 1, 2, 3 });

    const p1 = acc1.prepare(old.prepare);
    acc1.state = p1.state;
    var result = proposer.promise(p1.promise.?);
    proposer.state = result.state;
    const p2 = acc2.prepare(old.prepare);
    acc2.state = p2.state;
    result = proposer.promise(p2.promise.?);
    proposer.state = result.state;
    try testing.expectEqual(0xcafe, result.accept.?.value);

    const a1 = acc1.accept(result.accept.?); // acc1 accepted 0xcafe
    acc1.state = a1.state;
    // assume accept messages for the rest of the nodes were dropped

    // acc4 will act as a proposer now
    const proposal = acc4.propose(0xdead);
    acc4.state = proposal.state;
    const p3 = acc3.prepare(proposal.prepare);
    acc3.state = p3.state;
    var accepted = acc4.promise(p3.promise.?);
    acc4.state = accepted.state;
    const p2b = acc2.prepare(proposal.prepare);
    acc2.state = p2b.state;
    accepted = acc4.promise(p2b.promise.?);
    acc4.state = accepted.state;
    try testing.expectEqual(0xdead, accepted.accept.?.value);

    const a2 = acc2.accept(accepted.accept.?);
    acc2.state = a2.state;

    // state
    // acc1 accepted = 0xcafe
    // acc2 accepted = 0xdead

    // acc3 will act as a proposer now
    acc3.state.round.counter += 10; // make it the highest so far
    const last = acc3.propose(0x6767);
    acc3.state = last.state;
    const p1b = acc1.prepare(last.prepare);
    acc1.state = p1b.state;
    result = acc3.promise(p1b.promise.?);
    acc3.state = result.state;
    const p2c = acc2.prepare(last.prepare);
    acc2.state = p2c.state;
    result = acc3.promise(p2c.promise.?);
    acc3.state = result.state;

    // the accepted was from the highest numbered proposal so far
    try testing.expectEqual(0xdead, result.accept.?.value);
}

test "a value is chosen once it is learned from the majority" {
    var learner: Paxos(u32, 5) = .init(0, &.{ 1, 2, 3, 4 });
    var proposer: Paxos(u32, 5) = .init(1, &.{ 0, 2, 3, 4 });
    var acc2: Paxos(u32, 5) = .init(2, &.{ 0, 1, 3, 4 });
    var acc3: Paxos(u32, 5) = .init(3, &.{ 0, 1, 2, 4 });
    var acc4: Paxos(u32, 5) = .init(4, &.{ 0, 1, 2, 3 });

    const proposal = proposer.propose(42);
    proposer.state = proposal.state;
    const p2 = acc2.prepare(proposal.prepare);
    acc2.state = p2.state;
    var result = proposer.promise(p2.promise.?);
    proposer.state = result.state;
    const p3 = acc3.prepare(proposal.prepare);
    acc3.state = p3.state;
    result = proposer.promise(p3.promise.?);
    proposer.state = result.state;
    const accept = result.accept.?;

    const a2 = acc2.accept(accept);
    acc2.state = a2.state;
    learner.state = learner.learn(a2.learn.?);
    try testing.expectEqual(null, learner.state.chosen);

    const a3 = acc3.accept(accept);
    acc3.state = a3.state;
    learner.state = learner.learn(a3.learn.?);
    try testing.expectEqual(null, learner.state.chosen);

    const a4 = acc4.accept(accept);
    acc4.state = a4.state;
    learner.state = learner.learn(a4.learn.?);
    try testing.expectEqual(42, learner.state.chosen);
}

test "it tolerates duplicate Learn requests" {
    var learner: Paxos(u32, 5) = .init(0, &.{ 1, 2, 3, 4 });
    var proposer: Paxos(u32, 5) = .init(1, &.{ 0, 2, 3, 4 });
    var acc2: Paxos(u32, 5) = .init(2, &.{ 0, 1, 3, 4 });
    var acc3: Paxos(u32, 5) = .init(3, &.{ 0, 1, 2, 4 });
    //var acc4: Paxos(u32) = .init(4, &.{ 0, 1, 2, 3 });

    const proposal = proposer.propose(42);
    proposer.state = proposal.state;
    const p2 = acc2.prepare(proposal.prepare);
    acc2.state = p2.state;
    var result = proposer.promise(p2.promise.?);
    proposer.state = result.state;
    const p3 = acc3.prepare(proposal.prepare);
    acc3.state = p3.state;
    result = proposer.promise(p3.promise.?);
    proposer.state = result.state;
    const accept = result.accept.?;

    const a2 = acc2.accept(accept);
    acc2.state = a2.state;
    learner.state = learner.learn(a2.learn.?);
    try testing.expectEqual(null, learner.state.chosen);

    const a3 = acc3.accept(accept);
    acc3.state = a3.state;
    learner.state = learner.learn(a3.learn.?);
    try testing.expectEqual(null, learner.state.chosen);

    learner.state = learner.learn(a3.learn.?);
    try testing.expectEqual(null, learner.state.chosen);
}

test "a proposer must re-propose a value it has already accepted" {
    var proposer: Paxos(u32, 3) = .init(0, &.{ 1, 2 });
    var acc1: Paxos(u32, 3) = .init(1, &.{ 0, 2 });
    var acc2: Paxos(u32, 3) = .init(2, &.{ 0, 1 });

    const first = proposer.propose(0xdeadbeef);
    proposer.state = first.state;
    const pr1 = acc1.prepare(first.prepare);
    acc1.state = pr1.state;
    var result = proposer.promise(pr1.promise.?);
    proposer.state = result.state;
    try testing.expectEqual(0xdeadbeef, result.accept.?.value);

    const a1 = acc1.accept(result.accept.?);
    acc1.state = a1.state;

    const second = proposer.propose(0xdead);
    proposer.state = second.state;
    const pr2 = acc2.prepare(second.prepare);
    acc2.state = pr2.state;
    result = proposer.promise(pr2.promise.?);
    proposer.state = result.state;
    try testing.expectEqual(0xdeadbeef, result.accept.?.value);
}
