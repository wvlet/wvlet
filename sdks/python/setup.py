#!/usr/bin/env python
#
# Copyright 2024 wvlet.org
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import glob
from setuptools import setup, find_packages

# Read the long description from README.md
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read version from pyproject.toml or define it here
version = "0.1.0"  # TODO: Extract from pyproject.toml or version file

# Find all native library files
def find_native_libs():
    """Find all native library files to include in the package."""
    lib_files = []
    lib_dir = "wvlet/libs"
    
    if os.path.exists(lib_dir):
        for root, dirs, files in os.walk(lib_dir):
            for file in files:
                if file.endswith(('.so', '.dylib', '.dll')):
                    # Get the relative path from the package root
                    rel_dir = os.path.relpath(root, 'wvlet')
                    lib_files.append((f'wvlet/{rel_dir}', [os.path.join(root, file)]))
    
    return lib_files

# Package data files including native libraries
package_data = {
    'wvlet': [
        'libs/linux_x86_64/*.so',
        'libs/linux_aarch64/*.so',
        'libs/darwin_arm64/*.dylib',
        'libs/.gitkeep',
    ]
}

setup(
    name="wvlet",
    version=version,
    author="wvlet.org",
    author_email="taro.saito@wvlet.org",
    description="Python SDK for Wvlet - A flow-style query language",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/wvlet/wvlet",
    project_urls={
        "Bug Tracker": "https://github.com/wvlet/wvlet/issues",
        "Documentation": "https://wvlet.org/",
        "Source Code": "https://github.com/wvlet/wvlet",
    },
    packages=find_packages(),
    package_data=package_data,
    include_package_data=True,
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Database",
        "Topic :: Software Development :: Compilers",
    ],
    # Include native libraries
    data_files=find_native_libs(),
)