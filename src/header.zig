const std = @import("std");

const HEADERMAGIC = "CHUNK   ";
const HEADERVERSION = 4;
const HEADERENDIAN = 0xa;

pub const Header = struct {
    const Self = @This();
    const HeaderData = packed struct {
        // data is always little endian because of the heritage.
        // often stores aren't directly moved so endianness never
        // comes up
        magic: u64,
        version: u32,
        unused1: u16,
        endianness: u16, // always 0xa meaning little
        arraysz: u64,
        datastart: u64,
        firstcommit: u64,
        currentcommit: u64,
        auxdata: u64,
        auxdatasz: u64,
    };
    header: *HeaderData,
    data: []u8,

    arraysz: *u64,
    datastart: *u64,
    firstcommit: *u64,
    currentcommit: *u64,
    auxdata: *u64,
    auxdatasz: *u64,

    fn make(allocator: *std.mem.Allocator) !*Self {
        const newheader = try allocator.create(Self);
        newheader.header = try allocator.create(HeaderData);
        newheader.data = std.mem.asBytes(newheader.header);
        @memset(newheader.data, 0);
        // convenience pointers
        newheader.header.* = .{
            .magic = std.mem.readVarInt(u64, HEADERMAGIC, .little),
            .version = HEADERVERSION,
            .unused1 = 0xADDE,
            .endianness = HEADERENDIAN,
            .arraysz = 0,
            .datastart = @sizeOf(HeaderData),
            .firstcommit = 0,
            .currentcommit = 0,
            .auxdata = 0,
            .auxdatasz = 0,
        };
        newheader.arraysz = &newheader.header.arraysz;
        newheader.datastart = &newheader.header.datastart;
        newheader.firstcommit = &newheader.header.firstcommit;
        newheader.currentcommit = &newheader.header.currentcommit;
        newheader.auxdata = &newheader.header.auxdata;
        newheader.auxdatasz = &newheader.header.auxdatasz;
        return newheader;
    }

    fn hread(self: *Self, file: *std.fs.File) !void {
        // i know about the grammar. read and read are the same to the
        // compiler
        const readed = try file.pread(self.data, 0);
        if (readed < self.data.len) return error.ShortRead;
    }

    fn hwrite(self: *Self, file: *std.fs.File) !void {
        const written = try file.pwrite(self.data, 0);
        if (written < self.data.len) return error.ShortWrite;
    }

    fn ewrite(self: *Self, file: *std.fs.File) !void {
        try file.seekFromEnd(0);
        const pos = try file.getPos();
        const written = try file.pwrite(self.data, pos);
        if (written < self.data.len) return error.ShortWrite;
    }

    pub fn destroy(self: *Self, allocator: *std.mem.Allocator) void {
        allocator.destroy(self.header);
        allocator.destroy(self);
    }

    // make a new object
    pub fn new(allocator: *std.mem.Allocator) !*Header {
        const newheader = try make(allocator);
        return newheader;
    }

    // make a new object and load an object from a file
    pub fn load(allocator: *std.mem.Allocator, file: *std.fs.File) !*Header {
        var newheader = try make(allocator);
        const headerint = std.mem.readVarInt(u64, HEADERMAGIC, .little);
        errdefer newheader.destroy(allocator);
        try newheader.hread(file);
        if (newheader.header.magic != headerint)
            return error.MalformedHeader;
        return newheader;
    }

    // write to a file. all objects have a commit method
    pub fn commit(self: *Self, file: *std.fs.File) !void {
        try self.ewrite(file);
        try self.hwrite(file);
    }

    // read from a file. all objects have a reload method
    pub fn reload(self: *Self, file: *std.fs.File) !void {
        try self.hread(file);
    }
};

test "header basic test" {
    var testfile = try std.fs.cwd().createFile(
        "header_testfile.bin",
        .{ .read = true },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    const testheader = try Header.new(&allocator);
    testheader.arraysz.* = 0xaaaa0000aaaaaaaa;
    testheader.firstcommit.* = 0x0123456789abcdef;
    testheader.currentcommit.* = 0xabcd;
    testheader.auxdata.* = 0xbc9a;
    testheader.auxdatasz.* = 0xefcd;
    std.debug.print("data start = {}\n", .{testheader.datastart.*});
    try testheader.commit(&testfile);
    testheader.destroy(&allocator);
}

test "header read test" {
    var testfile = try std.fs.cwd().openFile(
        "header_testfile.bin",
        .{ .mode = .read_write },
    );
    defer testfile.close();
    var allocator = std.testing.allocator;
    const testheader = try Header.load(&allocator, &testfile);
    try std.testing.expect(testheader.arraysz.* == 0xaaaa0000aaaaaaaa);
    try std.testing.expect(testheader.firstcommit.* == 0x0123456789abcdef);
    try std.testing.expect(testheader.currentcommit.* == 0xabcd);
    try std.testing.expect(testheader.auxdata.* == 0xbc9a);
    try std.testing.expect(testheader.auxdatasz.* == 0xefcd);
    testheader.destroy(&allocator);
}
