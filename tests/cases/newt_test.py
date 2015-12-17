import unittest
import argparse
import json
import StringIO

import requests
import httmock

from cumulus.transport import get_connection
from cumulus.transport.newt import NewtException

newt_base_url = 'https://newt.nersc.gov/newt'

class NewtClusterConnectionTestCase(unittest.TestCase):

    def _me(self, url, request):
        content = {
            'newt': {
                'sessionId': self.session_id
            }
        }
        content = json.dumps(content)
        headers = {
            'content-length': len(content),
            'content-type': 'application/json'
        }

        return httmock.response(200, content, headers, request=request)

    def setUp(self):
        status_url = '%s/login' % newt_base_url

        data = {
            'username': NewtClusterConnectionTestCase.USER,
            'password': NewtClusterConnectionTestCase.PASSWORD
        }

        r = requests.post(status_url, data=data)
        json_resp = r.json()

        self.assertTrue(json_resp['auth'])
        self.session_id = json_resp['newt_sessionid']

        self._cluster = {
            'type': 'newt',
            'config': {
                'host': 'cori'
            }
        }
        self._girder_token = 'dummy'

        def _me(url, request):
            return self._me(url, request)

        url = '/api/v1/user/me'
        self.me = httmock.urlmatch(
            path=r'^%s$' % url, method='GET')(_me)

        self.scratch_dir = '/global/cscratch1/sd/%s' % NewtClusterConnectionTestCase.USER
        self.test_data = 'nothing to see here!'
        self.test_case_dir = '%s/cumulus' % self.scratch_dir
        self.test_file_path = '%s/test.txt' % self.test_case_dir
        self.test_dir = '%s/cumulus' % self.test_case_dir

        # Create directory for test case
        with httmock.HTTMock(self.me):
            with get_connection(self._girder_token, self._cluster) as conn:
                conn.mkdir(self.test_case_dir)

    def tearDown(self):
        try:
            with httmock.HTTMock(self.me):
                with get_connection(self._girder_token, self._cluster) as conn:
                    conn.execute('rm -rf %s' % self.test_case_dir)
        except Exception:
            pass

    def test_put_get(self):
        stream = StringIO.StringIO(self.test_data)
        with httmock.HTTMock(self.me):
            with get_connection(self._girder_token, self._cluster) as conn:
                conn.put(stream, self.test_file_path)
                with conn.get(self.test_file_path) as get_stream:
                    self.assertEqual(get_stream.read(), self.test_data)

    def test_is_file(self):
        stream = StringIO.StringIO(self.test_data)
        with httmock.HTTMock(self.me):
                with get_connection(self._girder_token, self._cluster) as conn:
                    conn.put(stream, self.test_file_path)
                    self.assertTrue(conn.isfile(self.test_file_path))
                    self.assertFalse(conn.isfile(self.test_case_dir))

    def test_mkdir(self):
        with httmock.HTTMock(self.me):
                with get_connection(self._girder_token, self._cluster) as conn:
                    conn.mkdir(self.test_dir)
                    self.assertFalse(conn.isfile(self.test_dir))

    def test_stat(self):
        with httmock.HTTMock(self.me):
            with get_connection(self._girder_token, self._cluster) as conn:
                conn.stat(self.test_case_dir)

    def test_execute(self):
        with httmock.HTTMock(self.me):
            with get_connection(self._girder_token, self._cluster) as conn:
                self.assertEqual(conn.execute('ls /bin/ls'), '/bin/ls')

    def test_remove(self):
        stream = StringIO.StringIO(self.test_data)
        with httmock.HTTMock(self.me):
            with get_connection(self._girder_token, self._cluster) as conn:
                conn.put(stream, self.test_file_path)
                self.assertTrue(conn.isfile(self.test_file_path))
                conn.remove(self.test_file_path)
                with self.assertRaises(NewtException) as cm:
                    conn.stat(self.test_file_path)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run NewtClusterConnectionTestCase')
    parser.add_argument('-u', '--user', help='', required=True)
    parser.add_argument('-p', '--password', help='', required=True)
    args = parser.parse_args()

    NewtClusterConnectionTestCase.USER = args.user
    NewtClusterConnectionTestCase.PASSWORD = args.password

    suite = unittest.TestLoader().loadTestsFromTestCase(NewtClusterConnectionTestCase)
    unittest.TextTestRunner().run(suite)
