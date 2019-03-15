# -*- coding: utf-8 -*-
# Copyright 2013 Google Inc. All Rights Reserved.
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
"""Integration tests for gsutil -D option."""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import platform

import six
import gslib
from gslib.cs_api_map import ApiSelector
import gslib.tests.testcase as testcase
from gslib.tests.testcase.integration_testcase import SkipForS3
from gslib.tests.util import ObjectToURI as suri
from gslib.tests.util import SetBotoConfigForTest
from gslib.utils.unit_util import ONE_KIB


@SkipForS3('-D output is implementation-specific.')
class TestDOption(testcase.GsUtilIntegrationTestCase):
  """Integration tests for gsutil -D option."""

  def test_minus_D_multipart_upload(self):
    """Tests that debug option does not output upload media body."""
    # We want to ensure it works with and without a trailing newline.
    for file_contents in (b'a1b2c3d4', b'a1b2c3d4\n'):
      fpath = self.CreateTempFile(contents=file_contents)
      bucket_uri = self.CreateBucket()
      with SetBotoConfigForTest(
          [('GSUtil', 'resumable_threshold', str(ONE_KIB))]):
        stderr = self.RunGsUtil(
            ['-D', 'cp', fpath, suri(bucket_uri)], return_stderr=True)
        print('command line:' + ' '.join(['-D', 'cp', fpath, suri(bucket_uri)]))
        if self.test_api == ApiSelector.JSON:
          self.assertIn('media body', stderr)
        self.assertNotIn('a1b2c3d4', stderr)
        self.assertIn('Comparing local vs cloud md5-checksum for', stderr)
        self.assertIn('total_bytes_transferred: %d' % len(file_contents),
                      stderr)

  def test_minus_D_perf_trace_cp(self):
    """Test upload and download with a sample perf trace token."""
    file_name = 'bar'
    fpath = self.CreateTempFile(file_name=file_name, contents=b'foo')
    bucket_uri = self.CreateBucket()
    stderr = self.RunGsUtil(['-D', '--perf-trace-token=123', 'cp', fpath,
                             suri(bucket_uri)], return_stderr=True)
    self.assertIn('\'cookie\': \'123\'', stderr)
    stderr2 = self.RunGsUtil(['-D', '--perf-trace-token=123', 'cp',
                              suri(bucket_uri, file_name), fpath],
                             return_stderr=True)
    self.assertIn('\'cookie\': \'123\'', stderr2)

  def test_minus_D_resumable_upload(self):
    fpath = self.CreateTempFile(contents=b'a1b2c3d4')
    bucket_uri = self.CreateBucket()
    with SetBotoConfigForTest([('GSUtil', 'resumable_threshold', '4')]):
      stderr = self.RunGsUtil(
          ['-D', 'cp', fpath, suri(bucket_uri)], return_stderr=True)
      self.assertNotIn('a1b2c3d4', stderr)
      self.assertIn('Comparing local vs cloud md5-checksum for', stderr)
      self.assertIn('total_bytes_transferred: 8', stderr)

  def test_minus_D_cat(self):
    """Tests cat command with debug option."""
    key_uri = self.CreateObject(contents=b'0123456789')
    with SetBotoConfigForTest([('Boto', 'proxy_pass', 'secret')]):
      (stdout, stderr) = self.RunGsUtil(
          ['-D', 'cat', suri(key_uri)], return_stdout=True, return_stderr=True)
    self.assertIn('You are running gsutil with debug output enabled.', stderr)
    self.assertIn("reply: 'HTTP/1.1 200 OK", stderr)
    self.assertIn('config:', stderr)
    self.assertIn("reply: 'HTTP/1.1 200 OK", stderr)
    # Headers come in different forms, depending on the python version. Adding
    # both options for full coverage and to ensure no false negatives.
    self.assertTrue(("('proxy_pass', u'REDACTED')" in stderr)
                    or ("('proxy_pass', 'REDACTED')" in stderr),
                    "('proxy_pass', 'REDACTED') not found in '{0}'".format(
                      stderr))
    self.assertTrue(('header: Expires: ' in stderr)
                    or ('Expires header: ' in stderr),
                    "Expires header not found in '{0}'".format(
                      stderr))
    self.assertTrue(('header: Date: ' in stderr)
                    or ('Date header: ' in stderr),
                    "Date header not found in '{0}'".format(
                      stderr))
    self.assertTrue(('header: Content-Type: application/octet-stream' in stderr)
                    or ('Content-Type header: ' in stderr),
                    "Content-Type header not found in '{0}'".format(
                      stderr))
    self.assertTrue(('header: Content-Length: 10' in stderr)
                    or ('Content-Length header: ' in stderr),
                    "Content-Length header not found in '{0}'".format(
                      stderr))

    if self.test_api == ApiSelector.XML:
      self.assertTrue(('header: Cache-Control: private, max-age=0' in stderr)
                      or ('Cache-Control header: ' in stderr),
                    "Cache-Control header not found in '{0}'".format(
                      stderr))
      self.assertTrue(('header: Last-Modified: ' in stderr)
                      or ('Last-Modified header: ' in stderr),
                    "Last-Modified header not found in '{0}'".format(
                      stderr))
      self.assertTrue(('header: ETag: "781e5e245d69b566979b86e28d23f2c7"' in
                       stderr) or ('ETag header:' in stderr),
                    "ETag header not found or incorrect value found in "
                    "'{0}'".format(stderr))
      self.assertTrue(('header: x-goog-generation: ' in stderr)
                      or ('x-goog-generation header:' in stderr),
                    "x-goog-generation header not found in '{0}'".format(
                      stderr))
      self.assertTrue(('header: x-goog-metageneration: 1' in stderr)
                      or ('x-goog-metageneration header:' in stderr),
                    "x-goog-metageneration header not found in '{0}'".format(
                      stderr))
      self.assertTrue(
          (('header: x-goog-hash: crc32c=KAwGng==' in stderr)
           and ('header: x-goog-hash: md5=eB5eJF1ptWaXm4bijSPyxw==' in stderr))
          or ('x-goog-hash header:' in stderr), "x-goog-hash header not found "
              "or incorrect value found in '{0}'".format(stderr))
      if six.PY2:
        self.assertRegex(
            stderr, '.*HEAD /%s/%s.*Content-Length: 0.*User-Agent: .*gsutil/%s' %
            (key_uri.bucket_name, key_uri.object_name, gslib.VERSION))
    elif self.test_api == ApiSelector.JSON:
      self.assertRegex(stderr,
                       '.*GET.*b/%s/o/%s.*user-agent:.*gsutil/%s.Python/%s' %
                       (key_uri.bucket_name, key_uri.object_name, gslib.VERSION,
                        platform.python_version()))
      self.assertTrue(('header: Cache-Control: no-cache, no-store, max-age=0,'
                       ' must-revalidate' in stderr)
                      or ('Cache-Control header: ' in stderr),
                    "JSON Cache-Control header not found or incorrect value "
                    "found in '{0}'".format(stderr))
      self.assertTrue(("md5Hash: u'eB5eJF1ptWaXm4bijSPyxw=='" in stderr)
                      or ("md5Hash: 'eB5eJF1ptWaXm4bijSPyxw=='" in stderr),
                    "md5 hash not found or incorrect value found in "
                    "'{0}'".format(stderr))

    if gslib.IS_PACKAGE_INSTALL:
      self.assertIn('PACKAGED_GSUTIL_INSTALLS_DO_NOT_HAVE_CHECKSUMS', stdout)
    else:
      self.assertRegex(stdout, r'.*checksum: [0-9a-f]{32}.*')
    self.assertIn('gsutil version: %s' % gslib.VERSION, stdout)
    self.assertIn('boto version: ', stdout)
    self.assertIn('python version: ', stdout)
    self.assertIn('OS: ', stdout)
    self.assertIn('multiprocessing available: ', stdout)
    self.assertIn('using cloud sdk: ', stdout)
    self.assertIn('pass cloud sdk credentials to gsutil: ', stdout)
    self.assertIn('config path(s): ', stdout)
    self.assertIn('gsutil path: ', stdout)
    self.assertIn('compiled crcmod: ', stdout)
    self.assertIn('installed via package manager: ', stdout)
    self.assertIn('editable install: ', stdout)
