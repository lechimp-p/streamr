# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

from setuptools import setup, find_packages, Command

description = "A stream abstraction for Python."
long_description = str(open("README.md", "rb").read())

setup( name = "streamr"
     , version = "0.1.0"
     , license = "MIT"
     , author = "Richard Klees"
     , url = "https://github.com/lechimp-p/streamr"
     , description = description
     , long_description = long_description
     , packages = find_packages()
     , install_requires = []
     #, entry_points = []
     #, classifiers = ()
     )

