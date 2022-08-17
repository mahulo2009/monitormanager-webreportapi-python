import requests
import logging

from mmwebreport.core.response import Response


class Executor(object):
    """
    A class used to represent a Http request to the Monitor Manager Web Report Backend
    """
    def __init__(self, uri):
        self._uri = uri

    def run(self):
        """
        Make a Http request to the Monitor Manager Web Report Backend and return a Response object. In case
        of HTTP request error it returns an exception.
        """
        logging.info(self._uri)
        r = requests.get(self._uri)
        if r.status_code == 200:
            return Response(r)
        else:
            # todo add more error conditions
            raise requests.exceptions.HTTPError
