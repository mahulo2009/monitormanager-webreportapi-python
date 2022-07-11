import re

from mmwebreport.core.executor import Executor

import json

_QUERY_DESCRIPTION_MONITOR_TOKEN = "monitors"
_QUERY_DESCRIPTION_MAGNITUDE_TOKEN = "magnitud"

_QUERY_MONITOR_TOKEN = "idmonitor"
_QUERY_MAGNITUDE_TOKEN = "idmagnitud"

_QUERY_PAGE_START_TOKEN = "page"
_QUERY_PAGE_LENGTH_TOKEN = "length"



_DEFAULT_SAMPLES_PER_PAGE = 30000

def _parse_raw_test(text):
    text = text.split('\n')

    text_header = ','.join(text[3].replace("/", ".").split(",")[1:])
    # todo extract all metadata
    text_header = re.sub(r"\(.*\)", "", text_header)

    text_body = [','.join(line.split(",")[1:]) for line in text[4:]]
    text_body = '\n'.join(text_body)

    return text_header, text_body


class Cursor(object):

    def __init__(self, base_url, url, pages, initial_page=0):
        self._base_url = base_url
        self._url = url
        self._current = initial_page
        self._pages = pages

    def __iter__(self):
        return self

    def __next__(self):
        if self._current >= self._pages:
            raise StopIteration

        url = self._base_url + "/download" + self._url + \
                    "&" + _QUERY_PAGE_START_TOKEN + "=" + str(self._current) + \
                    "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        executor = Executor(url)
        samples = executor.run()

        header, body = _parse_raw_test(samples)
        self._current += 1

        return header + '\n' + body

    def size(self):
        return self._pages

class Report(object):

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._base_url = "http://" + self._host + ":" + self._port + "/WebReport/rest"

    def get_components(self):
        url = self._base_url + "/components"
        executor = Executor(url)
        components = executor.run()

        return json.loads(components)

    def get_component(self, q_component):
        url = self._base_url + "/components/" + q_component
        executor = Executor(url)
        component = executor.run()

        return json.loads(component)

    def get_monitor(self, q_component, q_monitor, q_type="monitor"):

        query_parsed = _QUERY_DESCRIPTION_MONITOR_TOKEN
        if q_type == "magnitude":
            query_parsed = _QUERY_DESCRIPTION_MAGNITUDE_TOKEN

        url = self._base_url + "/components/" + q_component + "/" + query_parsed + "/" + q_monitor
        executor = Executor(url)
        monitor = executor.run()

        return json.loads(monitor)

    def _parse_search(self, q_date_ini, q_date_end):
        search_uri_part = ""
        search_uri_part = search_uri_part + q_date_ini.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        search_uri_part = search_uri_part + q_date_end.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        search_uri_part = search_uri_part + "/0?"

        return search_uri_part

    def _parse_single_monitor(self, monitor, q_type="monitor"):

        query_parsed = ""
        if q_type == "monitor":
            query_parsed = _QUERY_MONITOR_TOKEN
        elif q_type == "magnitude":
            query_parsed = _QUERY_MAGNITUDE_TOKEN
        query_parsed = query_parsed + "=" + str(monitor["id"])

        return query_parsed

    def _search_monitor_description(self, q_data_ini, q_data_end, q_component, monitor, q_type="monitor"):
        url = self._base_url + "/search/metadata"
        url = url + self._parse_search(q_data_ini, q_data_end)
        url = url + self._parse_single_monitor(monitor, q_type)
        url = url + "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        executor = Executor(url)
        description = executor.run()

        return json.loads(description)

    def search_monitor_description(self, q_data_ini, q_data_end, q_component, q_monitor, q_type="monitor"):

        monitor = self.get_monitor(q_component, q_monitor)
        description = self._search_monitor_description(q_data_ini, q_data_end, q_component, monitor, q_type)

        return description

    def search(self, q_data_ini, q_data_end, q_component, q_monitor, q_type="monitor"):

        monitor = self.get_monitor(q_component, q_monitor, q_type)

        search_monitor_description \
            = self._search_monitor_description(q_data_ini, q_data_end, q_component, monitor, q_type)

        url = self._parse_search(q_data_ini, q_data_end)
        url = url + self._parse_single_monitor(monitor, q_type)

        a_cursor = Cursor(self._base_url, url, search_monitor_description["totalPages"])

        return a_cursor
