import requests


class Executor(object):

    def __init__(self, uri):
        self._uri = uri

    def run(self):
        print(self._uri)
        r = requests.get(self._uri)
        if r.status_code == 200:
            return r.text
        else:
            # todo check this
            raise requests.exceptions.HTTPError
