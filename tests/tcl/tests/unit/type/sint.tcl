# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

start_server {tags {"sorted-int"}} {
    test {basic function of sint} {
        assert_equal 1 [r SIADD mysi 1]
        assert_equal 1 [r SIADD mysi 2]
        assert_equal 0 [r SIADD mysi 2]
        assert_equal 5 [r SIADD mysi 3 4 5 123 245]

        assert_equal 7 [r SICARD mysi]
        assert_equal {245 123 5} [r SIREVRANGE mysi 0 3]
        assert_equal {4 3 2} [r SIREVRANGE mysi 0 3 cursor 5]
        assert_equal {245} [r SIRANGE mysi 0 3 cursor 123]
        assert_equal {1 2 3 4} [r SIRANGEBYVALUE mysi 1 (5]
        assert_equal {5 4 3 2} [r SIREVRANGEBYVALUE mysi 5 (1]
        assert_equal {1 0 1} [r SIEXISTS mysi 1 88 2]
        assert_equal {1} [r SIREM mysi 2]
    }
}
