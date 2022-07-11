from unittest import TestCase

import requests

from mmwebreport.core.executor import Executor


class TestExecutor(TestCase):

    def test_run_ok(self):

        executor = Executor("http://calp-vwebrepo:8081/WebReport/rest/components")
        components = executor.run()
        assert components

    def test_run_no_ok(self):

        try:
            executor = Executor("http://calp-vwebrepo:8081/WebReport/rest")
            components = executor.run()
        except requests.exceptions.HTTPError:
            assert True




