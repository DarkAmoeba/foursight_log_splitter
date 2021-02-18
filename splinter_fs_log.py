#!/usr/bin/env python
"""
Take the input file and split it into chunks of an specified size
"""
from __future__ import print_function
from datetime import datetime
import argparse
import gzip
import logging
import os
import re
import sys

logging.basicConfig(level=logging.INFO)

MESSAGE_START = b'([0-9A-F]{4}\s[0-9A-F]{4}\s[0-9A-F]{16}\s{2}TSMS)'
REGEX_LEN = 5 + 5 + 18 + 4 - 1
PATTERN = re.compile(MESSAGE_START)


def input_stream(fname=None):
    """
    return a bytesIO object to that provides a read() operation
    """
    if fname:
        in_stream = gzip.open(fname, 'rb') if fname.endswith('.gz') else open(fname, 'rb')
    else:
        in_stream = sys.stdin.buffer
    return in_stream


def split_at_message(in_stream):
    """
    This function reads a fix number of bytes into a buffer and looks
    for the start of messages in the buffer.  It outputs the buffer divided
    at the frist message boundary.
    """
    buff = b''
    while True:
        match = PATTERN.search(buff, REGEX_LEN)
        if match:
            idx = match.start()
            return buff[:idx], buff[idx:]
        else:
            chunk = in_stream.read(1024)
            if not chunk:
                return buff, None
            else:
                buff += chunk


def get_msg_time(byte_string):
    """
    Function extracts the walltime for the first message in the byte string passed.
    if byte string is empty it retuns the empty string
    """
    if not byte_string:
        return ''

    Wallclock_Time_String = byte_string[10:26]   # Integer [18] uS since 1970                    (2 byte padding)
    Wallclock_Time = int(Wallclock_Time_String, 16) / 1000000.0
    return datetime.utcfromtimestamp(Wallclock_Time).strftime('%Y%m%d-%H%M%S')


def split_bytes(fname, prefix, size, compression):
    """
    Open an input stream and read a fixed amount of data (size)
    into a new temporary file.
    Then begin scanning for a message header and write all content upto the next
    header into the temp file.
    Rename the temp file with the prefix and start and end times of the messages
    written to the file.
    """
    file_buf = input_stream(fname)
    residual = b''
    i = 1
    start_t = ''
    end_t = ''

    # files from indra start with a header so strip it off
    end_of_last_msg, residual = split_at_message(file_buf)
    start_t = get_msg_time(residual)
    if end_of_last_msg:
        logging.info('Message header found and discarded:\n' + end_of_last_msg)

    # open the file, read a chuck of size bytes.
    # into a new file
    print('Part written: ', end='')
    while True:
        temp_file = 'temp_part_%s.gz' % str(i).zfill(5)
        f = gzip.open(temp_file, 'wb', compression)
        chunk = file_buf.read(size)
        f.write(residual)
        f.write(chunk)

        end_of_last_msg, residual = split_at_message(file_buf)
        end_t = get_msg_time(residual)
        f.write(end_of_last_msg)
        f.close()
        os.rename(temp_file, '_'.join([prefix, start_t, end_t, '.gz']))
        print(i)

        start_t = end_t
        if residual is None:
            return # End of stream
        i += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('file', type=str)
    parser.add_argument('-p', '--prefix', type=str, default='FOURSIGHT', help='the prefix to use on the output file ')
    parser.add_argument('-m', '--megabytes', type=int, default=50, help='the chucksize in megabytes')
    parser.add_argument('-c', '--compression', type=int, default=5, help='compression for gzip 1 fastest to 9 slowest (most compression)')
    args = parser.parse_args()

    split_bytes(args.file, args.prefix, args.megabytes * 1024 * 1024, args.compression)
