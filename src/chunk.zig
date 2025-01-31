const std = @import("std");
const ArrayPointer = @import("array.zig").ArrayPointer;
const Array = @import("array.zig").Array;
const ChunkStore = @import("chunkstore.zig").ChunkStore;

pub const Chunk = struct {
    const Self = @This();
    pointer: *ArrayPointer,
    idx: u64,
    data: []u8,
    allocated: bool,
    file: ?*std.fs.File,
    parent: ?*ChunkStore,

    fn make(
        allocator: *std.mem.Allocator,
        array: *Array,
        elem: u64,
        data: []u8,
    ) !*Self {
        if (elem >= array.pointers.len) return error.OutOfBounds;
        const newchunk = try allocator.create(Chunk);
        newchunk.* = .{
            .file = null,
            .pointer = &array.pointers[elem],
            .idx = elem,
            .data = data,
            .allocated = false,
            .parent = null,
        };
        return newchunk;
    }

    pub fn destroy(self: *Self, allocator: *std.mem.Allocator) []u8 {
        if (self.parent) |chunkstore| {
            chunkstore.chunks[self.idx].ptr = null;
            chunkstore.chunkref -= 1;
        }
        const data = self.data;
        allocator.destroy(self);
        return data;
    }

    pub fn destroy_noret(self: *Self, allocator: *std.mem.Allocator) void {
        _ = self.destroy(allocator);
    }

    fn destroy_unallocated_noret(self: *Self, allocator: *std.mem.Allocator) void {
        _ = self.destroy(allocator);
    }

    pub fn destroy_allocated(self: *Self, allocator: *std.mem.Allocator) void {
        if (self.parent) |chunkstore| {
            chunkstore.chunks[self.idx].ptr = null;
            chunkstore.chunkref -= 1;
        }
        allocator.free(self.data);
        allocator.destroy(self);
    }

    pub fn Parent(self: *Self) *ChunkStore {
        const rparent = self.parent orelse @panic("parentless chunk!");
        return rparent;
    }

    fn read(self: *Self, fd: *std.fs.File) !void {
        if (self.data.len < self.pointer.len) return error.BufferTooSmall;
        const readed = try fd.pread(self.data, self.pointer.offset);
        if (readed < self.data.len) return error.ShortRead;
    }

    fn write(self: *Self, fd: *std.fs.File) !void {
        if (self.data.len < self.pointer.len) return error.BufferTooSmall;
        const written = try fd.pwrite(self.data, self.pointer.offset);
        if (written < self.data.len) return error.ShortWrite;
    }

    pub fn replace(self: *Self, newdata: []u8) ![]u8 {
        const olddata = self.data;
        self.data = newdata;
        self.pointer.len = self.data.len;
        return olddata;
    }

    pub fn new(
        allocator: *std.mem.Allocator,
        array: *Array,
        fd: *std.fs.File,
        elem: u64,
        size: u64,
    ) !*Self {
        const newdata = try allocator.alloc(u8, size);
        errdefer allocator.free(newdata);
        const newchunk = try make(allocator, array, elem, newdata);
        newchunk.allocated = true;
        newchunk.pointer.len = newdata.len;
        newchunk.file = fd;
        return newchunk;
    }

    pub fn newAllocated(
        allocator: *std.mem.Allocator,
        array: *Array,
        fd: *std.fs.File,
        elem: u64,
        data: []u8,
    ) !*Self {
        const newchunk = try make(allocator, array, elem, data);
        newchunk.pointer.len = data.len;
        newchunk.file = fd;
        return newchunk;
    }

    pub fn loadAllocated(
        allocator: *std.mem.Allocator,
        array: *Array,
        fd: *std.fs.File,
        elem: u64,
        data: []u8,
    ) !*Self {
        if (data.len < array.pointers[elem].len) return error.BufferTooSmall;
        const newchunk = try make(allocator, array, elem, data);
        errdefer newchunk.destroy_unallocated_noret(allocator);
        try newchunk.read(fd);
        newchunk.file = fd;
        return newchunk;
    }

    pub fn load(
        allocator: *std.mem.Allocator,
        array: *Array,
        fd: *std.fs.File,
        elem: u64,
    ) !*Chunk {
        const newdata = try allocator.alloc(u8, array.pointers[elem].len);
        errdefer allocator.free(newdata);
        const newchunk = try loadAllocated(allocator, array, fd, elem, newdata);
        newchunk.allocated = true;
        return newchunk;
    }

    pub fn commitFile(self: *Self, fd: *std.fs.File) !void {
        try fd.seekFromEnd(0);
        const curstart = try fd.getPos();
        self.pointer.offset = curstart;
        try self.write(fd);
    }

    pub fn commit(self: *Self) !void {
        if (self.file) |fd| {
            try self.commitFile(fd);
        } else {
            return error.NoFile;
        }
    }
};

test "chunk write test" {
    const Header = @import("header.zig").Header;
    const teststring1 = "hello world";
    const teststring2 = "this is a test";
    var testfile = try std.fs.cwd().createFile(
        "chunk_testfile.bin",
        .{ .read = true },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    var testheader = try Header.new(&allocator);
    defer testheader.destroy(&allocator);
    testheader.arraysz.* = 10;
    try testheader.commit(&testfile);
    var testarray = try Array.new(&allocator, testheader);
    defer testarray.destroy(&allocator);
    var testchunk1 = try Chunk.new(&allocator, testarray, &testfile, 4, teststring1.len);
    var testchunk2 = try Chunk.newAllocated(
        &allocator,
        testarray,
        &testfile,
        2,
        @constCast(teststring2[0..]),
    );
    @memcpy(testchunk1.data[0..teststring1.len], teststring1);
    try testchunk1.commit();
    try testchunk2.commit();
    try testarray.commit(&testfile);
    try testheader.commit(&testfile);
    testchunk1.destroy_allocated(&allocator);
    _ = testchunk2.destroy(&allocator);
}

test "chunk read test" {
    const Header = @import("header.zig").Header;
    const teststring3 = "a change";
    var testfile = try std.fs.cwd().openFile(
        "chunk_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    var testheader = try Header.load(&allocator, &testfile);
    defer testheader.destroy(&allocator);
    var testarray = try Array.load(&allocator, testheader, &testfile);
    defer testarray.destroy(&allocator);
    const testchunk1 = try Chunk.load(&allocator, testarray, &testfile, 4);
    const testchunk2 = try Chunk.load(&allocator, testarray, &testfile, 2);
    std.debug.print("testchunk1.data = {s}\n", .{testchunk1.data});
    std.debug.print("testchunk2.data = {s}\n", .{testchunk2.data});
    const old = try testchunk1.replace(@constCast(teststring3[0..]));
    defer allocator.free(old);
    try testchunk2.commit();
    try testchunk1.commit();
    try testarray.commit(&testfile);
    try testheader.commit(&testfile);
    _ = testchunk1.destroy(&allocator);
    testchunk2.destroy_allocated(&allocator);
}

test "chunk read test 2" {
    const Header = @import("header.zig").Header;
    const teststring3 = "more changes!";
    var testfile = try std.fs.cwd().openFile(
        "chunk_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    var testheader = try Header.load(&allocator, &testfile);
    defer testheader.destroy(&allocator);
    var testarray = try Array.load(&allocator, testheader, &testfile);
    defer testarray.destroy(&allocator);
    const testchunk1 = try Chunk.load(&allocator, testarray, &testfile, 4);
    const testchunk2 = try Chunk.load(&allocator, testarray, &testfile, 2);
    std.debug.print("testchunk1.data = {s}\n", .{testchunk1.data});
    std.debug.print("testchunk2.data = {s}\n", .{testchunk2.data});
    const old = try testchunk1.replace(@constCast(teststring3[0..]));
    defer allocator.free(old);
    try testchunk2.commit();
    try testchunk1.commit();
    try testarray.commit(&testfile);
    try testheader.commit(&testfile);
    _ = testchunk1.destroy(&allocator);
    testchunk2.destroy_allocated(&allocator);
}

test "chunk super write test" {
    const Header = @import("header.zig").Header;
    var testfile = try std.fs.cwd().createFile(
        "chunk_supertestfile.bin",
        .{ .read = true },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    var testheader = try Header.new(&allocator);
    defer testheader.destroy(&allocator);
    testheader.arraysz.* = 20;
    try testheader.commit(&testfile);
    var testarray = try Array.new(&allocator, testheader);
    defer testarray.destroy(&allocator);
    try testarray.commit(&testfile);
    for (0..testarray.pointers.len) |i| {
        const testvalue: u64 = 0xaddeffffffff0000 + i;
        const testbytes = @constCast(std.mem.asBytes(&testvalue));
        var testchunk = try Chunk.newAllocated(
            &allocator,
            testarray,
            &testfile,
            i,
            testbytes[0..],
        );
        try testchunk.commit();
        if (i % 5 == 0) {
            try testarray.commit(&testfile);
            try testheader.commit(&testfile);
        }
        _ = testchunk.destroy(&allocator);
    }
    try testarray.commit(&testfile);
    try testheader.commit(&testfile);
}
