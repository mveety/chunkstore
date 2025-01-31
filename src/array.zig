const std = @import("std");
const Header = @import("header.zig").Header;

pub const ArrayPointer = packed struct {
    offset: u64,
    len: u64,
};

pub const Array = struct {
    const Self = @This();
    header: *Header,
    offset: u64,
    len: u64,
    lastarray: *ArrayPointer,
    pointers: []ArrayPointer,
    allpointers: []ArrayPointer,
    ptrbuf: []u8,

    fn make(allocator: *std.mem.Allocator, nelem: u64) !*Self {
        const newarray = try allocator.create(Self);
        errdefer allocator.destroy(newarray);
        newarray.allpointers = try allocator.alloc(ArrayPointer, nelem + 1);
        newarray.lastarray = &newarray.allpointers[0];
        newarray.pointers = newarray.allpointers[1..];
        newarray.ptrbuf = std.mem.sliceAsBytes(newarray.allpointers);
        @memset(newarray.ptrbuf, 0);
        return newarray;
    }

    pub fn destroy(self: *Self, allocator: *std.mem.Allocator) void {
        allocator.free(self.allpointers);
        allocator.destroy(self);
    }

    fn read(self: *Self, file: *std.fs.File, offset: u64) !void {
        // don't get on me about grammar. i know.
        const readed = try file.pread(self.ptrbuf, offset);
        if (readed < self.ptrbuf.len) return error.ShortRead;
    }

    fn write(self: *Self, file: *std.fs.File, offset: u64) !void {
        const written = try file.pwrite(self.ptrbuf, offset);
        if (written < self.ptrbuf.len) return error.ShortWrite;
    }

    pub fn resize(self: *Self, allocator: *std.mem.Allocator, newsize: u64) !void {
        if (newsize < self.pointers.len) return error.TooSmall;
        if (newsize == self.pointers.len) return;
        const oldallpointers = self.allpointers;
        var newallpointers = try allocator.alloc(ArrayPointer, newsize + 1);
        const tmpbytes = std.mem.sliceAsBytes(newallpointers);
        @memset(tmpbytes, 0);
        for (oldallpointers, 0..) |oldelem, i| newallpointers[i] = oldelem;
        self.allpointers = newallpointers;
        self.lastarray = &self.allpointers[0];
        self.pointers = self.allpointers[1..];
        self.ptrbuf = std.mem.sliceAsBytes(self.allpointers);
        self.header.arraysz.* = self.ptrbuf.len;
        allocator.free(oldallpointers);
    }

    pub fn elem(self: *Self, n: u64) !*ArrayPointer {
        if (n >= self.pointer.len) return error.OutOfBounds;
        return &self.pointers[n];
    }

    pub fn size(self: *Self) u64 {
        return self.pointer.len;
    }

    pub fn new(allocator: *std.mem.Allocator, fileheader: *Header) !*Array {
        const newarray = try make(allocator, fileheader.arraysz.*);
        newarray.header = fileheader;
        newarray.offset = 0;
        newarray.len = fileheader.arraysz.*;
        return newarray;
    }

    pub fn load(
        allocator: *std.mem.Allocator,
        fileheader: *Header,
        file: *std.fs.File,
    ) !*Array {
        const newarray = try make(allocator, fileheader.arraysz.*);
        errdefer newarray.destroy(allocator);
        newarray.header = fileheader;
        newarray.offset = fileheader.currentcommit.*;
        newarray.len = fileheader.arraysz.*;
        try newarray.read(file, newarray.offset);
        return newarray;
    }

    pub fn reload(self: *Self, file: *std.fs.File) !void {
        try self.read(file, self.offset);
    }

    pub fn commit(self: *Self, file: *std.fs.File) !void {
        try file.seekFromEnd(0);
        const curstart = try file.getPos();
        self.lastarray.offset = self.offset;
        self.lastarray.len = self.len;
        try self.write(file, curstart);
        self.header.currentcommit.* = curstart;
        if (self.header.firstcommit.* == 0)
            self.header.firstcommit.* = curstart;
        self.offset = curstart;
    }
};

test "array basic test" {
    var testfile = try std.fs.cwd().createFile(
        "array_testfile.bin",
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
    try testarray.commit(&testfile);
    for (0..testarray.pointers.len) |i| {
        testarray.pointers[i].offset = i;
        testarray.pointers[i].len = i;
    }
    try testarray.commit(&testfile);
    try testheader.commit(&testfile);
}

test "array write test" {
    var testfile = try std.fs.cwd().openFile(
        "array_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    var testheader = try Header.load(&allocator, &testfile);
    defer testheader.destroy(&allocator);
    var testarray = try Array.load(&allocator, testheader, &testfile);
    defer testarray.destroy(&allocator);
    for (testarray.pointers, 0..) |ap, i| {
        try std.testing.expect(ap.offset == i);
        try std.testing.expect(ap.len == i);
    }
    try testarray.commit(&testfile);
    try testheader.commit(&testfile);
}

test "array resize test" {
    var testfile = try std.fs.cwd().openFile(
        "array_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    var testheader = try Header.load(&allocator, &testfile);
    defer testheader.destroy(&allocator);
    var testarray = try Array.load(&allocator, testheader, &testfile);
    defer testarray.destroy(&allocator);
    try testarray.resize(&allocator, 20);
    for (10..testarray.pointers.len) |i| {
        testarray.pointers[i].offset = i;
        testarray.pointers[i].len = i;
    }
    try testarray.commit(&testfile);
    try testheader.commit(&testfile);
}
