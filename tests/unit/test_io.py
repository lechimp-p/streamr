# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr.io import read_file
from streamr.simple import ListP, ListC
from test_core import _TestPipe
import os

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

    def test_noFile(self):
        pass
     
