# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright MonetDB Solutions B.V. 2018-2019

from setuptools import setup, find_packages

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.rst')) as f:
    long_description = f.read()

setup(
    name="mal_analytics",
    version="0.3.0",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type='text/x-rst',
    install_requires=['monetdblite>=0.6.3'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    include_package_data=True,
    author="Panagiotis Koutsourakis",
    author_email="panagiotis.koutsourakis@monetdbsolutions.com",
    url="https://github.com/MonetDBSolutions/mal_analytics",
    classifiers = [
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha'
    ],
)
