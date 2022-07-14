import requests

from mmwebreport.core.response import Response


class Executor(object):

    def __init__(self, uri):
        self._uri = uri

    def run(self):

        print(self._uri)
        r = requests.get(self._uri)
        if r.status_code == 200:
            return Response(r)
        else:
            # todo check this
            raise requests.exceptions.HTTPError
