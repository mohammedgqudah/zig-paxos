const std = @import("std");
const Paxos = @import("Paxos.zig");
const BoundedArray = @import("BoundedArray.zig");
const Io = std.Io;
const mem = std.mem;

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const gpa = init.gpa;
    const args = try init.minimal.args.toSlice(arena);

    _ = args;
    _ = gpa;
}

test {
    std.testing.refAllDecls(Paxos);
    std.testing.refAllDecls(BoundedArray);
}
