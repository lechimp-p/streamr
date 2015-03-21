# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .simple import pipe 
from .core import Stop, Resume, MayResume, Exhausted
import json
import sys

###############################################################################
#
# Some pipes for input and output 
#
###############################################################################

@pipe(str, str)
def read_file(await, send):
    """
    Open the file with name given by upstream, read it completely and send it
    downstream.

    Will drop filenames that can't be opened.

    Uses utf-8 in py3 only.
    """
    try:
        is_py2 = sys.version_info[0] < 3
        v = await()
        with open(v, "r") if is_py2 else open(v, "r", encoding = "utf-8") as f:
            send(f.read())
    except IOError:
        pass
    return MayResume()


# TODO: This most probably does not belong here.
@pipe(str, dict)
def to_json(await, send):
    """
    Parse string from upstream as json.

    Will drop strings that can't be parsed as json.
    """
    try:
        send(json.loads(await()))
        return MayResume()
    except ValueError:
        pass
    return MayResume()
