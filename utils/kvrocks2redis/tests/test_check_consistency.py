#!/usr/bin/env python3

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

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import check_consistency as checker


class FailureDiagnosticsTest(unittest.TestCase):
    def comparator(self):
        return checker.RedisComparator("127.0.0.1", 6666, "", "127.0.0.1", 6379, "")

    def test_original_retry_budget_and_no_further_writes(self):
        comparator = self.comparator()
        comparator.src_cli = Mock()
        comparator.src_cli.type.return_value = "string"
        comparator._compare_data = Mock(return_value=("value_0", None))
        comparator._fail_with_diagnostics = Mock(side_effect=AssertionError("original mismatch"))
        with patch.object(checker.time, "sleep") as sleep:
            with self.assertRaisesRegex(AssertionError, "original mismatch"):
                comparator._import_and_compare(100)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [0.02, 0.1, 0.1, 0.1, 0.1])
        self.assertEqual(comparator._compare_data.call_count, 4)
        comparator.src_cli.set.assert_called_once_with("key_0", "value_0")
        comparator._fail_with_diagnostics.assert_called_once_with("key_0", "string", "value_0", None)

    def test_stalled_worker_is_stopped_and_original_failure_retained(self):
        process = Mock()
        process.is_alive.side_effect = [True, True, False]
        with patch.object(checker.multiprocessing, "get_context") as context, patch("builtins.print"):
            context.return_value.Process.return_value = process
            with self.assertRaisesRegex(AssertionError, "source data: 'value_49' destination data: 'None'"):
                self.comparator()._fail_with_diagnostics("key_49", "string", "value_49", None)
        self.assertEqual([call.args[0] for call in process.join.call_args_list], [10, 0.5, 0.5])
        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        process.close.assert_called_once()

    def test_diagnostic_start_error_does_not_replace_failure(self):
        with patch.object(checker.multiprocessing, "get_context", side_effect=OSError("unavailable")), patch("builtins.print"):
            with self.assertRaisesRegex(AssertionError, "Data mismatch for key 'key_49'"):
                self.comparator()._fail_with_diagnostics("key_49", "string", "value_49", None)

    def test_expired_source_is_not_convergence(self):
        comparator = Mock()
        comparator._compare_data.return_value = (None, None)
        now = [0.0]
        def advance(seconds):
            now[0] = 10.0
        with patch.object(checker, "RedisComparator", return_value=comparator), \
                patch.object(checker, "_sync_files", return_value={}), \
                patch.object(checker.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(checker.time, "sleep", side_effect=advance), patch("builtins.print") as output:
            checker._observe_failure((), ".", "__namespace", "key_49", "string", "value_49")
        snapshot = output.call_args.args[1]
        self.assertFalse(snapshot["source_unchanged"])
        self.assertFalse(snapshot["converged"])

    def test_checkpoint_and_bounded_aof_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "last_next_seq.txt").write_bytes(b"42 \x00")
            (root / "__namespace_last_next_offset.txt").write_bytes(b"128 \x00")
            aof = root / "__namespace_appendonly.aof"
            aof.write_bytes(b"*3\r\n$3\r\nSET\r\n$6\r\nkey_49\r\n$8\r\nvalue_49\r\n")
            snapshot = checker._sync_files(directory, "__namespace", "key_49")
            self.assertEqual(snapshot["next_seq"], "42")
            self.assertEqual(snapshot["sender_offset"], "128")
            self.assertIn("value_49", snapshot["aof_context"])
            aof.write_bytes(b"x" * (1024 * 1024 + 1))
            snapshot = checker._sync_files(directory, "__namespace", "key_49")
            self.assertEqual(snapshot["aof_scanned_bytes"], 1024 * 1024)
            self.assertEqual(snapshot["aof_key_bulk_offset"], -1)


if __name__ == "__main__":
    unittest.main()
