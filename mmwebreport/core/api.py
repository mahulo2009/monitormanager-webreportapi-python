from mmwebreport.core.executor import Executor

_QUERY_DESCRIPTION_MONITOR_TOKEN = "monitors"
_QUERY_DESCRIPTION_MAGNITUDE_TOKEN = "magnitud"

_QUERY_MONITOR_TOKEN = "idmonitor"
_QUERY_MAGNITUDE_TOKEN = "idmagnitud"

_QUERY_PAGE_START_TOKEN = "page"
_QUERY_PAGE_LENGTH_TOKEN = "length"

_DEFAULT_SAMPLES_PER_PAGE = 30000

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

        url = self._base_url + self._url + \
              "&" + _QUERY_PAGE_START_TOKEN + "=" + str(self._current) + \
              "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        self._current += 1

        return Executor(url)

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
        response = executor.run()

        return response.to_json()

    def get_component(self, q_component):
        url = self._base_url + "/components/" + q_component
        executor = Executor(url)
        response = executor.run()

        return response.to_json()

    def get_monitor(self, q_component, q_monitor, q_type="monitor"):

        query_parsed = _QUERY_DESCRIPTION_MONITOR_TOKEN
        if q_type == "magnitude":
            query_parsed = _QUERY_DESCRIPTION_MAGNITUDE_TOKEN

        url = self._base_url + "/components/" + q_component + "/" + query_parsed + "/" + q_monitor
        executor = Executor(url)
        response = executor.run()

        return response.to_json()

    def _parse_search(self, q_date_ini, q_date_end):

        search_uri_part = ""
        search_uri_part = search_uri_part + q_date_ini.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        search_uri_part = search_uri_part + q_date_end.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        # todo this is sampled period
        search_uri_part = search_uri_part + "/0?"

        return search_uri_part

    # todo this new method will replace _parser_search when the sampling period is better specified
    def _parse_search_clean(self, q_date_ini, q_date_end):

        search_uri_part = ""
        search_uri_part = search_uri_part + q_date_ini.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        search_uri_part = search_uri_part + q_date_end.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        search_uri_part = search_uri_part + "?"

        return search_uri_part

    def _parse_single_monitor(self, monitor, q_type):

        # todo exception if q_type not found
        query_parsed = ""
        if q_type == "monitor":
            query_parsed = _QUERY_MONITOR_TOKEN
        elif q_type == "magnitude":
            query_parsed = _QUERY_MAGNITUDE_TOKEN
        query_parsed = query_parsed + "=" + str(monitor["id"])

        return query_parsed

    def _parse_monitors(self, a_query):

        query_monitor_uri = ""

        if isinstance(a_query, (list, tuple)):
            for q in a_query:
                m = self.get_monitor(q["component"].replace('.', '/'), q["monitor"], q["type"])
                query_monitor_uri = query_monitor_uri + self._parse_single_monitor(m, q["type"]) + "&"
            query_monitor_uri = query_monitor_uri[:-1]
        else:
            m = self.get_monitor(a_query["component"].replace('.', '/'), a_query["monitor"], a_query["type"])

            query_monitor_uri = self._parse_single_monitor(a_query, m["type"])

        return query_monitor_uri

    def _search_description(self, q_data_ini, q_data_end, q_query):

        url = self._base_url + "/search/metadata"
        url = url + self._parse_search(q_data_ini, q_data_end)
        url = url + self._parse_monitors(q_query)
        url = url + "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        executor = Executor(url)
        response = executor.run()

        return response.to_json()

    def _search(self, q_data_ini, q_data_end, q_component, q_monitor, q_type="monitor"):

        query = [{"component": q_component, "monitor": q_monitor, "epsilon": 0.5, "type": q_type}]
        return self.search(q_data_ini, q_data_end, query)

    def search_description(self, q_data_ini, q_data_end, q_component, q_monitor, q_type="monitor"):

        query = [{"component": q_component, "monitor": q_monitor, "epsilon": 0.5, "type": q_type}]
        return self._search_description(q_data_ini, q_data_end, query)

    def search(self, q_data_ini, q_data_end, q_query):

        search_monitor_description \
            = self._search_description(q_data_ini, q_data_end, q_query)

        url = "/download"
        url = url + self._parse_search(q_data_ini, q_data_end)
        url = url + self._parse_monitors(q_query)

        a_cursor = Cursor(self._base_url, url, search_monitor_description["totalPages"])

        return a_cursor

    def search_stored_description(self, q_data_ini, q_data_end, q_query_name):

        url = self._base_url + "/query/metadata/"
        url = url + q_query_name
        url = url + self._parse_search_clean(q_data_ini, q_data_end)
        url = url + "&" + _QUERY_PAGE_START_TOKEN + "=" + str(0)
        url = url + "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        executor = Executor(url)
        response = executor.run()

        return response.to_json()

    def search_stored_query(self, q_data_ini, q_data_end, q_query_name):

        search_monitor_description \
            = self.search_stored_description(q_data_ini, q_data_end, q_query_name)

        url = "/query/download/"
        url = url + q_query_name
        url = url + self._parse_search_clean(q_data_ini, q_data_end)
        url = url + "&" + _QUERY_PAGE_START_TOKEN + "=" + str(0)
        url = url + "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        a_cursor = Cursor(self._base_url, url, search_monitor_description["totalPages"])

        return a_cursor
