# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr.io import read_file, to_json
from streamr.simple import ListP, ListC
from test_core import _TestPipe
import os
import json

class TestReadFile(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return read_file 

    @pytest.fixture
    def test_values(self, tmpdir):
        files = [tmpdir.join("foo%d" % i) for i in range(0,10)]
        for f in files:
            f.write("Hm, okay, what's this: %s" % f)
        return [f.strpath for f in files]

    @pytest.fixture
    def result(self, test_values):
        return [open(fn, "r").read() for fn in test_values]

    def test_noFile(self, pipe):
        pr = ListP(["/this/most/probably/wont/point/to/a/file"])
        co = ListC()

        sp = pr >> pipe >> co
        res = sp.run()
        assert res == []
     
class TestToJSON(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return to_json 

    @pytest.fixture
    def test_values(self, result):
        return [json.dumps(d) for d in result]

    @pytest.fixture
    def result(self):
        return [ {}
               , { "foo" : "bar" }
               , { "foo" : 3 }
               , { "foo" : { "bar" : "baz" } }
               ]
