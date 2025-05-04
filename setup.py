#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import re

with open("README.md") as readme_file:
    readme = readme_file.read()


filepath = "qsig/__init__.py"
version_file = open(filepath)
(__version__,) = re.findall('__version__ = "(.*)"', version_file.read())

requirements = ["pandas"]

setup(
    name="qsig",
    version=__version__,
    python_requires=">=3.6",
    packages=find_packages(),
    scripts=[],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    description=("Quant & Systematic Crypto Research Tools"),
    install_requires=requirements,
    license="MIT",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="cryptocurrency, binance",
)
