# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from .simple import pipe 
from .core import MayResume
import re 

###############################################################################
#
# Some pipes for string manipulation 
#
###############################################################################

def search(pattern):
    """
    Search for a regex pattern in a string and get all matches.
    """
    @pipe(str, [str])
    def search(await, send):
        send(re.findall(pattern, await()))
        return MayResume()
    return search

@pipe((str, [(str, str)]), str)
def replace(await, send):
    """
    Replace some strings in another string.
    """
    st, repls = await()
    for o,n in repls:
        st = st.replace(o,n)
    send(st)
    return MayResume()
   
