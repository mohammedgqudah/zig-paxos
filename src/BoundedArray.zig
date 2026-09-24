pub fn Bounded(comptime T: type, comptime capacity: usize) type {
    return struct {
        buffer: [capacity]T = undefined,
        len: usize = 0,

        const Self = @This();

        pub fn initOne(item: T) Self {
            var self = Self{};
            self.buffer[0] = item;
            self.len = 1;
            return self;
        }

        pub fn slice(self: *Self) []T {
            return self.buffer[0..self.len];
        }

        pub fn append(self: *Self, item: T) !void {
            if (self.len >= capacity) return error.OutOfMemory;
            self.buffer[self.len] = item;
            self.len += 1;
        }
    };
}
