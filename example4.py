from streamr import StreamProcessor, MayResume, from_list, to_list

import re

class SearchWithRegex(StreamProcessor):
    """
    Give a regular expression as init value. Take strings from upstream
    and send the number the regex matched on the string downstream. Provide
    the total amount of matches a result.

    Regex -> int % string -> int
    
    This is the processor from the typed example above.
    """
    def __init__(self):
        re_type = type(re.compile(".")) 
        super(SearchWithRegex, self).__init__(re_type, int, str, int)

    def setup(self, params, result):
        result(0)
        return (params[0], [0])

    def step(self, env, stream):
        # Stream is an object that provides the methods await, send and result. 
        string = stream.await()
        amount = len(env[0].findall(string))
        env[1][0] += amount
        stream.result(env[1][0])
        stream.send(amount)
        return MayResume

searchWithRegex = SearchWithRegex()

sp = from_list(["1a33efg", "1", "a"]) >> searchWithRegex >> to_list()
assert sp.run(re.compile("\d")) == (4, [3,1,0])
