# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

__all__ = []


def _exception_message(excp):
    """Return the message from an exception as either a str or unicode object.  Supports both
    Python 2 and Python 3.

    >>> msg = "Exception message"
    >>> excp = Exception(msg)
    >>> msg == _exception_message(excp)
    True

    >>> msg = u"unicöde"
    >>> excp = Exception(msg)
    >>> msg == _exception_message(excp)
    True
    """
    if hasattr(excp, "message"):
        return excp.message
    return str(excp)

class whileLoop:
    def __init__(self, sc, continueCheck):
        self._sc = sc
        self._continueCheck = continueCheck
        self._loopId = sc._startLoop()
        self._first = True

    def __iter__(self):
        return self

    def next(self):
        if not self._continueCheck():
            self._sc._endLoop(self._loopId)
            raise StopIteration()
        else:
            if not self._first:
                self._sc._iterateLoop(self._loopId)
            self._first = False
            return None

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        exit(-1)
