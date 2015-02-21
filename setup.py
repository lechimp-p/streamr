# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import os
from setuptools import setup, find_packages, Command

base_path = os.path.dirname(os.path.realpath(__file__))

description = "A stream abstraction for Python."
readme = os.path.join(base_path, "README.md")
long_description = open(readme, "rb").read()

setup( name = "streamr"
     , version = "0.1.0"
     , license = "MIT"
     , author = "Richard Klees"
     , url = "https://github.com/lechimp-p/streamr"
     , description = description
     , long_description = long_description
     , packages = find_packages()
     , install_requires = []
     )
