#!/usr/bin/python
"""
Pattern:
	a 32 bit integer, network byte order specifying the piece size `n'
	the piece: n bytes of data (read from the input stream)
	the sha256 up until this point.
"""
from contextlib import contextmanager
import os, hashlib, struct, shutil
piece_size = 4 * 1024**2 # 4MB
magic = b'reliable-encap'


class FileExists(Exception):
    pass


class IntegrityError(ValueError):
    pass


def yield_pieces(input_file):
    hasher = hashlib.sha256()
    yield magic
    while True:
        buf = input_file.read(piece_size)
        hasher.update(buf)
        yield struct.pack('!I', len(buf))
        yield buf
        yield hasher.digest()
        if not buf:
            break


def yield_input(input_file):
    hasher = hashlib.sha256()
    if not input_file.read(len(magic)) == magic:
        raise IntegrityError("Magic number missing")
    while True:
        length, = struct.unpack('!I', input_file.read(struct.calcsize('!I')))
        buf = input_file.read(length)
        hasher.update(buf)
        cur_hash = input_file.read(hasher.digest_size)
        if not hasher.digest() == cur_hash:
            raise IntegrityError("Hash Mismatch")
        if not buf:
            break
        yield buf


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
    import sys, os, shutil
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
