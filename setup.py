from setuptools import setup, find_packages

setup(
    name="malpar",
    version="0.1.dev0",
    packages=find_packages(),
    setup_requires=['pytest-runner'],
    tests_require=['pytest']
)
