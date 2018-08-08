#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  Copyright 2018 Kitware Inc.
#
#  Licensed under the Apache License, Version 2.0 ( the "License" );
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###############################################################################

from tests import base
import json
from bson.objectid import ObjectId

import cumulus
from cumulus.testing import AssertCallsMixin


def setUpModule():
    base.enabledPlugins.append('taskflow')
    base.startServer()


def tearDownModule():
    base.stopServer()


class TaskFlowTestCase(AssertCallsMixin, base.TestCase):
    def setUp(self):
        super(TaskFlowTestCase, self).setUp()

        users = ({
            'email': 'admin@email.com',
            'login': 'admin',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword',
            'admin': True
        }, {
            'email': 'regularuser@email.com',
            'login': 'regularuser',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        }, {
            'email': 'another@email.com',
            'login': 'another',
            'firstName': 'First',
            'lastName': 'Last',
            'password': 'goodpassword'
        })
        self._admin_user, self._user, self._another_user = \
            [self.model('user').createUser(**user) for user in users]

    # This tests both "ls" and "ls all"
    def test_ls(self):
        body = {
            'taskFlowClass': 'cumulus.taskflow.core.test.mytaskflows.SimpleTaskFlow',
            'name': 'test_taskflow'
        }

        json_body = json.dumps(body)

        # Make two taskflows using two different users
        r = self.request('/taskflows', method='POST', type='application/json',
                         body=json_body, user=self._user)
        self.assertStatus(r, 201)
        taskflow_id1 = r.json['_id']

        r = self.request('/taskflows', method='POST', type='application/json',
                         body=json_body, user=self._another_user)
        self.assertStatus(r, 201)
        taskflow_id2 = r.json['_id']

        # Only the first taskflow id should be present for the first user
        r = self.request('/taskflows', method='GET', user=self._user)
        self.assertStatusOk(r)
        self.assertTrue(len(r.json) == 1)
        self.assertTrue(r.json[0]['_id'] == taskflow_id1)

        # Only the second taskflow id should be present for the second user
        r = self.request('/taskflows', method='GET', user=self._another_user)
        self.assertStatusOk(r)
        self.assertTrue(len(r.json) == 1)
        self.assertTrue(r.json[0]['_id'] == taskflow_id2)
