# Copyright (C) 2015 Richard Klees <richard.klees@rwth-aachen.de>

import os
from setuptools import setup, find_packages, Command

base_path = os.path.dirname(os.path.realpath(__file__))

description = "A stream abstraction."
readme = os.path.join(base_path, "README.md")
long_description = str(open(readme, "rb").read())

setup( name = "streamr"
     , version = "0.8.0"
     , license = "MIT"
     , author = "Richard Klees"
     , author_email = "richard.klees@rwth-aachen.de"
     , url = "https://github.com/lechimp-p/streamr"
     , description = description
     , long_description = long_description
     , packages = find_packages()
     , install_requires = []
     , classifiers = [ "Development Status :: 4 - Beta"
                     , "Intended Audience :: Developers"
                     , "Topic :: Data Processing :: Streams"
                     , "License :: OSI Approved :: MIT License"
                     , "Programming Language :: Python :: 2.7"
                     , "Programming Language :: Python :: 3.2"
                     , "Programming Language :: Python :: 3.3"
                     , "Programming Language :: Python :: 3.4"
                     ]
     , keywords = "data processing stream streams"
     )
