#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os

__version__ = '0.1.2'

if __name__ == '__main__':
    from setuptools import find_packages, setup  #

    setup(
        name="PyDO",
        version=__version__,
        description='Python DataBase Objects',
        author='Bobby4k',
        url='https://github.com/bobby4k/PyDO',
        install_requires=['dsnparse'],
        # test dirs
        packages=find_packages(
            exclude=['packages', 'test'],
        ),
    )
