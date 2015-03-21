# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import pytest

from streamr.string import search, replace 
from streamr.simple import ListP, ListC
from test_core import _TestPipe
import os
import json

class TestSearch(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return search("\d+")

    @pytest.fixture
    def test_values(self):
        return ["12, foo bar bla, 2, 3"]

    @pytest.fixture
    def result(self, test_values):
        return [["12", "2", "3"]]

    def test_group(self):
        lp = ListP(["__1__"])
        lc = ListC()
        sp = lp >> search("__(\d)__") >> lc
        assert sp.run() == [["1"]]
     
class TestReplace(_TestPipe):
    @pytest.fixture
    def pipe(self):
        return replace

    @pytest.fixture
    def test_values(self, result):
        return [("abcd", [("a", "b"), ("c", "d")])]

    @pytest.fixture
    def result(self):
        return [ "bbdd" ]
