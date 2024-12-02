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

"""
Wvlet SDK for Python
==========================

Wvlet SDK for Python provides an interface to compile Wvlet code into SQL queries of some target platforms.

Typical usage
-------------

  from wvlet.compiler import WvletCompiler
  c = WvletCompiler()
  sql = c.compile("show tables")


By translating an Wvlet query into SQL, we can utilize existing code assets like SQLAlchemy and Pandas (e.g. pd.read_sql).

"""
