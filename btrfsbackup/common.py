import struct


magic_number = "\xa8\x5b\x4b\x2b\x1b\xf7\x4c\x0a"


def write_framed(pack_format, fh, buf):
    fh.write(struct.pack(pack_format, len(buf)) + buf)


def read_framed(pack_format, fh):
    header = fh.read(struct.calcsize(pack_format))
    return fh.read(struct.unpack(pack_format, header)[0])
