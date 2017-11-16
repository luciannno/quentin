#!/usr/bin/env python

# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='quentin',
    version='0.1.0',
    description='Another python quantitative analysis software',
    long_description=readme,
    author='Luciano Brunette',
    author_email='luciano@brunette.me.uk',
    url='https://github.com/luciano/quentin',
    license=license,
    packages=find_packages(exclude=('tests', 'docs', 'bin')), #[ 'quentin', 'dataaccess' ]
    package_dir={ 'quentin' : 'src' }
)
