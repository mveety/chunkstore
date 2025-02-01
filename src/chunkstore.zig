const std = @import("std");
const Header = @import("header.zig").Header;
const Array = @import("array.zig").Array;
const Chunk = @import("chunk.zig").Chunk;

pub const ChunkStore = struct {
    const Self = @This();

    const TrackedChunk = struct {
        ptr: ?*Chunk,
    };

    allocator: *std.mem.Allocator,
    file: *std.fs.File,
    header: *Header,
    array: *Array,
    chunkref: u64,
    chunks: []TrackedChunk,

    pub fn create(alloc: *std.mem.Allocator, fd: *std.fs.File, arraysz: u64) !*Self {
        const newcs = try alloc.create(Self);
        errdefer alloc.destroy(newcs);
        const newheader = try Header.new(alloc);
        errdefer alloc.destroy(newheader);
        newheader.arraysz.* = arraysz;
        const newarray = try Array.new(alloc, newheader);
        errdefer newarray.destroy(alloc);
        try newheader.commit(fd);
        try newarray.commit(fd);
        try newheader.commit(fd);
        const newchunks = try alloc.alloc(TrackedChunk, arraysz);
        for (0..newchunks.len) |i| newchunks[i].ptr = null;
        newcs.* = .{
            .allocator = alloc,
            .file = fd,
            .header = newheader,
            .array = newarray,
            .chunkref = 0,
            .chunks = newchunks,
        };
        return newcs;
    }

    pub fn open(alloc: *std.mem.Allocator, fd: *std.fs.File) !*Self {
        const newcs = try alloc.create(Self);
        errdefer alloc.destroy(newcs);
        const newheader = try Header.load(alloc, fd);
        errdefer newheader.destroy(alloc);
        const newarray = try Array.load(alloc, newheader, fd);
        errdefer newarray.destroy(alloc);
        const newchunks = try alloc.alloc(TrackedChunk, newarray.pointers.len);
        for (0..newchunks.len) |i| newchunks[i].ptr = null;
        newcs.* = .{
            .allocator = alloc,
            .file = fd,
            .header = newheader,
            .array = newarray,
            .chunkref = 0,
            .chunks = newchunks,
        };
        return newcs;
    }

    pub fn resize(self: *Self, newsize: u64) !void {
        try self.array.resize(self.allocator, newsize);
        const newchunks = try self.allocator.alloc(TrackedChunk, newsize);
        const oldchunks = self.chunks;
        defer self.allocator.free(oldchunks);
        for (0..newchunks.len) |i| {
            newchunks[i].ptr = null;
            if (i < self.chunks.len) {
                var chunk = self.chunks[i].ptr orelse continue;
                chunk.pointer = &self.array.pointers[i];
                newchunks[i].ptr = chunk;
            }
        }
        self.chunks = newchunks;
    }

    pub fn size(self: *Self) usize {
        return self.array.pointers.len;
    }

    pub fn Allocator(self: *Self) *std.mem.Allocator {
        return self.allocator;
    }

    pub fn File(self: *Self) *std.fs.File {
        return self.file;
    }

    pub fn commit(self: *Self) !void {
        try self.array.commit(self.file);
        try self.header.commit(self.file);
    }

    pub fn commitChunks(self: *Self) !void {
        for (self.chunks) |chunk| {
            if (chunk.ptr) |ptr| try ptr.commit();
        }
    }

    pub fn commitAll(self: *Self) !void {
        try self.commitChunks();
        try self.commit();
    }

    pub fn destroyUnsafe(self: *Self) void {
        self.allocator.free(self.chunks);
        self.array.destroy(self.allocator);
        self.header.destroy(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn destroy(self: *Self) !void {
        for (self.chunks) |chunk| {
            if (chunk.ptr) |_| return error.OpenChunks;
        }
        self.destroyUnsafe();
    }

    pub fn close(self: *Self) !void {
        try self.commit();
        try self.destroy();
    }

    pub fn chunksize(self: *Self, elem: u64) !u64 {
        if (elem >= self.array.pointers.len) return error.OutOfBounds;
        return self.array.pointers[elem].len;
    }

    pub fn allocateChunk(self: *Self, alloc: *std.mem.Allocator, elem: u64) ![]u8 {
        if (elem >= self.array.pointers.len) return error.OutOfBounds;
        return alloc.alloc(u8, self.array.pointers[elem].len);
    }

    pub fn chunkify(self: *Self, elem: u64, data: []u8) !*Chunk {
        const nc = try Chunk.newAllocated(
            self.allocator,
            self.array,
            self.file,
            elem,
            data,
        );
        nc.parent = self;
        self.chunkref += 1;
        self.chunks[elem].ptr = nc;
        return nc;
    }

    pub fn openChunk(self: *Self, elem: u64, buffer: []u8) !*Chunk {
        const nc = try Chunk.loadAllocated(
            self.allocator,
            self.array,
            self.file,
            elem,
            buffer,
        );
        nc.parent = self;
        self.chunkref += 1;
        self.chunks[elem].ptr = nc;
        return nc;
    }
};

test "chunkstore create test" {
    const teststring1 = "hello world";
    const teststring2 = "this is a test";
    var testfile = try std.fs.cwd().createFile(
        "chunkstore_testfile.bin",
        .{ .read = true },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    const newcs = try ChunkStore.create(&allocator, &testfile, 10);
    const testchunk1 = try newcs.chunkify(1, @constCast(teststring1[0..]));
    const testchunk2 = try newcs.chunkify(5, @constCast(teststring2[0..]));
    try newcs.commitAll();
    _ = testchunk1.destroy(testchunk1.Parent().Allocator());
    _ = testchunk2.destroy(testchunk2.Parent().Allocator());
    try newcs.destroy();
}

test "chunkstore read test" {
    var testfile = try std.fs.cwd().openFile(
        "chunkstore_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    const newcs = try ChunkStore.open(&allocator, &testfile);
    defer newcs.destroyUnsafe();
    const testslice1 = try newcs.allocateChunk(&allocator, 1);
    defer allocator.free(testslice1);
    const testslice2 = try newcs.allocateChunk(&allocator, 5);
    defer allocator.free(testslice2);
    const testchunk1 = try newcs.openChunk(1, testslice1);
    defer testchunk1.destroy_noret(testchunk1.Parent().Allocator());
    const testchunk2 = try newcs.openChunk(5, testslice2);
    defer testchunk2.destroy_noret(testchunk2.Parent().Allocator());
    std.debug.print("testslice1 = {s}\n", .{testslice1});
    std.debug.print("testslice2 = {s}\n", .{testslice2});
}

test "chunkstore write test" {
    const teststring1 = "a change";
    const teststring2 = "a really really really big change!";
    var testfile = try std.fs.cwd().openFile(
        "chunkstore_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    const newcs = try ChunkStore.open(&allocator, &testfile);
    defer newcs.destroyUnsafe();
    const testchunk1 = try newcs.chunkify(1, @constCast(teststring1[0..]));
    defer testchunk1.destroy_noret(testchunk1.Parent().Allocator());
    const testchunk2 = try newcs.chunkify(4, @constCast(teststring2[0..]));
    defer testchunk2.destroy_noret(testchunk2.Parent().Allocator());
    try newcs.commitAll();
}

test "chunkstore read2 test" {
    var testfile = try std.fs.cwd().openFile(
        "chunkstore_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    const newcs = try ChunkStore.open(&allocator, &testfile);
    defer newcs.destroyUnsafe();
    const testslice1 = try newcs.allocateChunk(&allocator, 1);
    defer allocator.free(testslice1);
    const testslice2 = try newcs.allocateChunk(&allocator, 5);
    defer allocator.free(testslice2);
    const testchunk1 = try newcs.openChunk(1, testslice1);
    defer testchunk1.destroy_noret(testchunk1.Parent().Allocator());
    const testchunk2 = try newcs.openChunk(5, testslice2);
    defer testchunk2.destroy_noret(testchunk2.Parent().Allocator());
    std.debug.print("testslice1 = {s}\n", .{testslice1});
    std.debug.print("testslice2 = {s}\n", .{testslice2});
}

test "chunkstore resize super test" {
    std.debug.print("started supertest\n", .{});
    var testfile = try std.fs.cwd().openFile(
        "chunkstore_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    const newcs = try ChunkStore.open(&allocator, &testfile);
    const testslice1_0 = try newcs.allocateChunk(&allocator, 1);
    defer allocator.free(testslice1_0);
    const testchunk1_0 = try newcs.openChunk(1, testslice1_0);
    std.debug.print("testslice1_0 = {s}\n", .{testslice1_0});
    try newcs.resize(20);
    try newcs.commitAll();
    _ = testchunk1_0.destroy(testchunk1_0.Parent().Allocator());
    for (0..newcs.array.pointers.len) |i| {
        const testvalue: u64 = 0xaddeffffffff0000 + i;
        const testbytes = @constCast(std.mem.asBytes(&testvalue));
        const testchunk = try newcs.chunkify(i, testbytes[0..]);
        try testchunk.commit();
        if (i % 5 == 0) try newcs.commit();
        _ = testchunk.destroy(testchunk.Parent().Allocator());
    }
    try newcs.close();
}
