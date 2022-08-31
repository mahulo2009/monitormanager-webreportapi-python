from mmwebreport.core.executor import Executor

_QUERY_DESCRIPTION_MONITOR_TOKEN = "monitors"
_QUERY_DESCRIPTION_MAGNITUDE_TOKEN = "magnitud"

_QUERY_MONITOR_TOKEN = "idmonitor"
_QUERY_MAGNITUDE_TOKEN = "idmagnitud"

_QUERY_PAGE_START_TOKEN = "page"
_QUERY_PAGE_LENGTH_TOKEN = "length"

_DEFAULT_SAMPLES_PER_PAGE = 30000


class Cursor(object):
    """
    A class used to iterate over the pages of a request response from the Monitor Manager Web Report Backend.
    Each pages will consist of a maximum of _DEFAULT_SAMPLES_PER_PAGE samples.

    It is important to notice that the iterator does not execute the request, instead an Executor is returned that
    must be explicit invoked to run the query.
    """

    def __init__(self, url, pages, initial_page=0):
        """
        Initialize the iterator.

        :param url: The Http URL for the request to Monitor Manager Web report backend.
        :param pages: The number of pages for the request response.
        :param initial_page: The initial page number.
        """
        # todo check consistency, if the initial page is lower than the number of pages
        self._url = url
        self._pages = pages
        self._initial_page = initial_page
        self._current = initial_page

    def __iter__(self):
        """
        Initialize the current page to the initial page

        :return: The iterator itself
        """
        self._current = self._initial_page
        return self

    def __next__(self):
        """
        Add to URL the page information and return an Executor object that can be used to invoke the request.

        :return: Executor object to make the final invocation
        """
        if self._current >= self._pages:
            raise StopIteration

        url = self._url + "&" + _QUERY_PAGE_START_TOKEN + "=" + str(self._current) + "&" + _QUERY_PAGE_LENGTH_TOKEN \
                        + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        self._current += 1

        return Executor(url)

    def size(self):
        """
        Accessor method to the size of iterator

        :return:  the number of pages.
        """
        return self._pages


class Report(object):
    """
    Invoke the monitor manager backend Api Rest:

        - Get list of components.
        - Get detail description for a component.
        - Get configuration of a monitor.
        - Return metadata for a query report: number of pages, samples per page...
        - Return a cursor to iterate over the samples result for a set of monitors inside an interval.
        - Return a cursor to iterate over the samples result for a named query inside an interval.

        A query is defined using a dictionary object as follow:

            query =
                [
                    {
                        "component": "",
                        "monitor": "",
                        "epsilon": ,
                        "type": ""
                    }
                ]

        Where:

            component:  The component name following GCS convention but replacing slash with dot. For example,
                        MACS.AzimuthAxis.

            monitor:    The monitor name following GCS convention. For example: position.

            epsilon:    If different from 0, a value used to filter consecutive similar samples from a query resutl.

            type:       The typ of monitor. It can be monitor for one-dimensional monitor, array for n-dimensional
                        monitor and magnitude for monitor of type enumerated.
    """

    def __init__(self, host, port):
        """
        Initialize the Request

        :param host: hostname for the monitor manager backend service.
        :param port: port  for the monitor manager backend service.
        """
        self._host = host
        self._port = port
        # build base url
        self._base_url = "http://" + self._host + ":" + self._port + "/WebReport/rest"

    def get_components(self):
        """
        Return the list of components
        """
        url = self._base_url
        url = url + "/components"

        executor = Executor(url)
        response = executor.run()

        return response.to_json()

    def get_component(self, q_component):
        """
        Given a component, return monitors and magnitudes for this component.

        :param q_component: the component name

        :return: the configuration of a component in json format. The output format is:

            {
                'id': ''
                'name': ''
                'className': ''
                'magnitudeDescription': []
                'monitorDescription' : []
            }

        """
        url = self._base_url
        url = url + "/components/"
        url = url + q_component

        executor = Executor(url)
        response = executor.run()

        return response.to_json()

    def get_monitor_configuration(self, q_component, q_monitor, q_type="monitor"):
        """
        Given a component and a monitor return the monitor configuration

        :param q_component: the component name.
        :param q_monitor: the monitor name
        :param q_type: the type, it can be monitor or magnitude

        :return: the monitor configuration in json format. The output format is:

            {
                'id': ''
                'magnitude: ''
                'dimension_x' : ''
                'dimension_y' : ''
                'description' : ''
                'type' : ''
                'unit' : ''
                'config' :  {
                                'id': ''
                                'active' : ''
                                'epsilon' : ''
                                'persite_by_default' : ''
                                'propagate_period' : ''
                                'storage_period' : ''
                                'monitorRanges' : {...}
                            }
            }
        """
        query_parsed = _QUERY_DESCRIPTION_MONITOR_TOKEN
        if q_type == "magnitude":
            query_parsed = _QUERY_DESCRIPTION_MAGNITUDE_TOKEN

        url = self._base_url
        url = url + "/components/"
        url = url + q_component + "/" + query_parsed + "/" + q_monitor

        executor = Executor(url)
        response = executor.run()

        return response.to_json()

    def search_info(self, q_data_ini, q_data_end, q_component, q_monitor, q_type="monitor"):
        """
        Given the parameters of a search request, return the metadata information about this query.

        :param q_data_ini: The search request initial date and time.
        :param q_data_end: The search request end date and time.
        :param q_component: The search request component.
        :param q_monitor: The search request monitor of the component.
        :param q_type: The search request type, it can be: monitor, array or magnitude.

        :return: the search request metadata in json format. The output format is:
                {
                    'length': ''
                    'totalPages: ''
                    'totalSamples' : ''
                }
        """
        query = [{"component": q_component, "monitor": q_monitor, "epsilon": 0.5, "type": q_type}]
        return self._search_info(q_data_ini, q_data_end, query)

    def search(self, q_data_ini, q_data_end, q_query):
        """
        Given the temporal parameters of a search request, and the query itself, return a Cursor to iterator over
        the query result. It is important to notice that the iterator does not execute the request, instead an Executor
        is returned that must be explicit invoked to run the query.

        :param q_data_ini: The search request initial date and time.
        :param q_data_end: The search request end date and time.
        :param q_query: The query itself has the following format:
            query =
                [
                    {
                        "component": "",
                        "monitor": "",
                        "epsilon": ,
                        "type": ""
                    }
                ]

        :return: a :Cursor to iterate over the result.
        """
        search_monitor_description \
            = self._search_info(q_data_ini, q_data_end, q_query)

        url = self._base_url
        url = url + "/download"
        url = url + self._parse_search(q_data_ini, q_data_end, sampling=0)
        url = url + self._parse_monitors(q_query)

        a_cursor = Cursor(url, search_monitor_description["totalPages"])

        return a_cursor

    def single_search(self, q_data_ini, q_data_end, q_component, q_monitor, q_type="monitor"):
        """
        Given the temporal parameters of a search request, and the query itself, return a Cursor to iterator over
        the query result. It is important to notice that the iterator does not execute the request, instead an Executor
        is returned that must be explicit invoked to run the query.

        :param q_data_ini: The search request initial date and time.
        :param q_data_end: The search request end date and time.
        :param q_component: The search request component name.
        :param q_monitor: The search request monitor name.
        :param q_type: The search request type, it can be: monitor, array or magnitude.

        :return: a :Cursor to iterate over the result.
        """
        query = [{"component": q_component, "monitor": q_monitor, "epsilon": 0.5, "type": q_type}]
        return self.search(q_data_ini, q_data_end, query)

    def search_stored_info(self, q_data_ini, q_data_end, q_query_name):
        """
        Given the temporal parameters of a search request, and the query stored name, return the metadata
        information about this query.

        :param q_data_ini: The search request initial date and time.
        :param q_data_end: The search request end date and time.
        :param q_query_name: The query stored name. This query must exits, and it can be created with the Monitor
        Manager Web Report frontend.

        :return: the search request metadata in json format. The output format is:
                {
                    'length': ''
                    'totalPages: ''
                    'totalSamples' : ''
                }
        """
        url = self._base_url
        url = url + "/query/metadata/"
        url = url + q_query_name
        url = url + self._parse_search(q_data_ini, q_data_end)
        url = url + "&" + _QUERY_PAGE_START_TOKEN + "=" + str(0)
        url = url + "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        executor = Executor(url)
        response = executor.run()

        return response.to_json()

    def search_stored_query(self, q_data_ini, q_data_end, q_query_name):
        """
        Given the temporal parameters of a search request, and the query stored name, return a Cursor to iterator over
        the query result. It is important to notice that the iterator does not execute the request, instead an Executor
        is returned that must be explicit invoked to run the query.

        :param q_data_ini: The search request initial date and time.
        :param q_data_end: The search request end date and time.
        :param q_query_name: The query stored name. This query must exits, and it can be created with the Monitor
        Manager Web Rerpot frontend.

        :return: a :Cursor to iterate over the result.
        """
        search_monitor_description \
            = self.search_stored_info(q_data_ini, q_data_end, q_query_name)

        url = self._base_url
        url = url + "/query/download/"
        url = url + q_query_name
        url = url + self._parse_search(q_data_ini, q_data_end)
        url = url + "&" + _QUERY_PAGE_START_TOKEN + "=" + str(0)
        url = url + "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        a_cursor = Cursor(url, search_monitor_description["totalPages"])

        return a_cursor

    def _parse_search(self, q_date_ini, q_date_end, sampling=None):
        """
        Build the URL part of a request related to the temporal part.

        :param q_date_ini: The search request initial date and time.
        :param q_date_end: The search request end date and time.
        :param sampling: The sampling period, of the moment only the default 0 is supported.

        :return: a string with the request related to the temporal part.

                The format is: /01/03/2022@23:50:00.000/02/03/2022@00:00:00.000/0?
        """
        search_uri_part = ""
        search_uri_part = search_uri_part + q_date_ini.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        search_uri_part = search_uri_part + q_date_end.strftime("/%d/%m/%Y@%H:%M:%S.%f")[:-3]
        if sampling is not None:
            search_uri_part = search_uri_part + "/" + str(sampling) + "?"
        else:
            search_uri_part = search_uri_part + "?"

        return search_uri_part

    def _parse_single_monitor(self, monitor, q_type):
        """
        Build the URL part of a request related to monitor.

        :param monitor: The search request monitor name of the component.
        :param q_type: The search request type, it can be: monitor array , or magnitude.

        :return: a string with the request related to the monitor part.

            The format is: idmonitor=3625
        """
        if q_type == "monitor":
            query_parsed = _QUERY_MONITOR_TOKEN + "=" + str(monitor["id"])
        elif q_type == "magnitude":
            query_parsed = _QUERY_MAGNITUDE_TOKEN + "=" + str(monitor["id"])
        elif q_type == "array":
            query_parsed = _QUERY_MONITOR_TOKEN + "=" + str(monitor["id"]) + "[[-1]]"
        else:
            raise "Type not supported"

        return query_parsed

    def _parse_monitors(self, a_query):
        """
        Build the URL part of a request related to a set of monitor.

        :param a_query: The query itself has the following format:
            query =
                [
                    {
                        "component": "",
                        "monitor": "",
                        "epsilon": ,
                        "type": ""
                    }
                ]
        :return: a string with the request related to a monitor set part.

            The format is: idmonitor=3623&idmonitor=3625
        """
        query_monitor_uri = ""

        if isinstance(a_query, (list, tuple)):
            for q in a_query:
                m = self.get_monitor_configuration(q["component"].replace('.', '/'), q["monitor"], q["type"])
                query_monitor_uri = query_monitor_uri + self._parse_single_monitor(m, q["type"]) + "&"
            query_monitor_uri = query_monitor_uri[:-1]
        else:
            m = self.get_monitor_configuration(a_query["component"].replace('.', '/'), a_query["monitor"],
                                               a_query["type"])

            query_monitor_uri = self._parse_single_monitor(a_query, m["type"])

        return query_monitor_uri

    def _search_info(self, q_data_ini, q_data_end, q_query):
        """
        Build the URL to make a request to obtain the metadata information about a query.

        :param q_data_ini: The search request initial date and time.
        :param q_data_end: The search request end date and time.
        :param q_query: The query itself has the following format:
            query =
                [
                    {
                        "component": "",
                        "monitor": "",
                        "epsilon": ,
                        "type": ""
                    }
                ]
        :return:  a string with the request URL to obtain metadata info for a query

            The format is: idmonitor=3623&idmonitor=3625
                .../search/metadata/01/03/2022@20:00:00.000/01/03/2022@20:00:10.000/0?
                        idmonitor=3623&
                        idmonitor=3625&
                        ...
                        length=30000
        """

        url = self._base_url + "/search/metadata"
        url = url + self._parse_search(q_data_ini, q_data_end, sampling=0)
        url = url + self._parse_monitors(q_query)
        url = url + "&" + _QUERY_PAGE_LENGTH_TOKEN + "=" + str(_DEFAULT_SAMPLES_PER_PAGE)

        executor = Executor(url)
        response = executor.run()

        return response.to_json()
