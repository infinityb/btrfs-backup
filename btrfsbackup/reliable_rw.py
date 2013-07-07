#!/usr/bin/python
"""
Pattern:
    magic number
    while we have data:
        a 32 bit integer, network byte order specifying the piece size `n'
        the piece: n bytes of data (read from the input stream)
        the sha256 up until this point.
    end magic number
"""
from contextlib import contextmanager
import os
import hashlib
import struct
import shutil


piece_size = 4 * 1024 ** 2  # 4MB


magic = b'reliable-encap'
end_magic = b'reliable-encap-end'


class FileExists(Exception):
    pass


class IntegrityError(ValueError):
    pass


def yield_pieces(input_file, with_magic=True):
    if with_magic:
        yield magic
    hasher = hashlib.sha256()
    while True:
        buf = input_file.read(piece_size)
        hasher.update(buf)
        yield struct.pack('!I', len(buf))
        yield buf
        yield hasher.digest()
        if not buf:
            break
    if with_magic:
        yield end_magic


@contextmanager
def yield_pieces_output_manager(output_file):
    output_file.write(magic)
    try:
        yield output_file
        output_file.write(end_magic)
    except:
        output_file.write("poison")
    finally:
        output_file.close()


def yield_input(input_file):
    hasher = hashlib.sha256()
    if not input_file.read(len(magic)) == magic:
        raise IntegrityError("Beginning magic number missing")
    while True:
        length, = struct.unpack('!I', input_file.read(struct.calcsize('!I')))
        buf = input_file.read(length)
        hasher.update(buf)
        cur_hash = input_file.read(hasher.digest_size)
        if not hasher.digest() == cur_hash:
            raise IntegrityError("Hash Mismatch")
        if length == 0:
            break
        yield buf
    if not input_file.read(len(end_magic)) == end_magic:
        raise IntegrityError("Terminating magic number missing")


@contextmanager
def transactional_write(filename):
    temp_file = '%s.tmp' % filename
    with open(temp_file, 'w') as fh:
        try:
            if os.path.exists(filename):
                raise FileExists()
            yield fh
            shutil.move(temp_file, filename)
        except:
            os.unlink(temp_file)
            raise


if __name__ == '__main__':
    import sys
    cmd = os.path.basename(sys.argv[0])
    if cmd == 'reliable-encap':
        for piece in yield_pieces(sys.stdin):
            sys.stdout.write(piece)
    elif cmd == 'reliable-write':
        with transactional_write(sys.argv[1]) as f:
            for orig_piece in yield_input(sys.stdin):
                f.write(orig_piece)
    else:
        raise Exception("Invalid Invocation: %s" % sys.argv[0])
