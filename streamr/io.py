# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .simple import pipe 
from .core import Stop, Resume, MayResume, Exhausted

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
    """
    with open(await()) as f:
        send(f.read())
    return MayResume()


# TODO: This most probably does not belong here.
@pipe(str, dict)
def to_json(await, send):
    """
    Parse string from upstream as json.

    Will drop strings that can't be parsed as json.
    """
    pass
