const std = @import("std");
const Io = std.Io;
const mem = std.mem;

pub fn main(init: std.process.Init) !void {
    const arena = init.arena.allocator();
    const gpa = init.gpa;
    const args = try init.minimal.args.toSlice(arena);

    _ = args;
    _ = gpa;
}
