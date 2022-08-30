import json
import os
from os.path import exists

import pandas as pd

_default_extension = 'csv.gz'

class Cache(object):
    def __init__(self, root_path, date_ini, date_end, query_name, a_query):
        self._a_id = a_query[0]['component'] + "." + a_query[0]['monitor']
        self._date_ini = date_ini
        self._date_end = date_end
        self._root_path = root_path
        self._query_name = query_name
        self._a_query = a_query


    def _make_path_root(self):
        path = self._root_path + "/" + \
               self._date_ini.strftime("%Y-%m-%d") + "/" + self._date_ini.strftime("%H")
        return path

    # todo mark as abstract method
    def make_path(self, page=None, extension=_default_extension):
        pass

    def _build_query(self, page=None):
        q = {
            "date_ini": self._date_ini.strftime("%Y-%m-%d %H_%M_%S"),
            "date_end": self._date_end.strftime("%Y-%m-%d %H_%M_%S"),
            "query_name": self._query_name,
            "query": self._a_query
        }
        if page is not None:
            q["page"] = page

        return q

    def compare_query(self, query_1, query_2, index=0):
        equal = True

        equal = equal and (query_1["date_ini"] == query_2["date_ini"])
        equal = equal and (query_1["date_end"] == query_2["date_end"])

        if len(query_1["query"]) != len(query_2["query"]):
            return False

        for index in range(0, len(query_1["query"])):
            equal = equal and (query_1["query"][index]["component"] == query_2["query"][index]["component"])
            equal = equal and (query_1["query"][index]["monitor"] == query_2["query"][index]["monitor"])
            equal = equal and (query_1["query"][index]["type"] == query_2["query"][index]["type"])
        # todo review this
        #    if q1["page"] and q2["page"]:
        #        equal = equal and (q1["page"] == q2["page"])
        return equal

    def _store_query(self, file_name):
        query = {
            "date_ini": self._date_ini.strftime("%Y-%m-%d %H_%M_%S"),
            "date_end": self._date_end.strftime("%Y-%m-%d %H_%M_%S"),
            "query_name": self._query_name,
            "query": self._a_query
        }
        if not exists(os.path.dirname(file_name)):
            os.makedirs(os.path.dirname(file_name))
        with open(file_name, 'w') as outfile:
            outfile.write(json.dumps(query))

    def store_query(self, page=''):
        file_name = self.make_path(page, extension='json')
        self._store_query(file_name)

    def read_query(self, index=0, page=''):
        _id = self._a_query[index]['component'] + "." + self._a_query[index]['monitor']
        file_name = self.make_path(page, extension='json')

        with open(file_name) as json_file:
            return json.load(json_file)

    def is_data_frame_cached(self, page=''):
        file_name_cached = self.make_path(page, extension='json')

        if not exists(file_name_cached):
            return False

        query_build = self._build_query(page)
        query_disk = self.read_query(0, page)

        return self.compare_query(query_build, query_disk)

    def data_frame_from_cache(self, page=''):
        file_name = self.make_path(page)
        return pd.read_csv(file_name, compression='infer', parse_dates=[0])

    def data_frame_to_cache(self, data_frame, page=''):
        file_name = self.make_path(page)
        if not exists(os.path.dirname(file_name)):
            os.makedirs(os.path.dirname(file_name))

        data_frame.to_csv(file_name, index=False, compression='infer')
        self.store_query(page)


class CacheRaw(Cache):
    def __init__(self, root_path, date_ini, date_end, query_name, a_query):
        super().__init__(root_path, date_ini, date_end, query_name, a_query)

    def make_path(self, page=None, extension=_default_extension):
        path = self._make_path_root() + "/" + str(self._a_id).replace(".", "/") + "/raw"

        file_name_monitor = self._date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            self._date_end.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            str(self._a_id)
        if page is None:
            file_name_monitor = file_name_monitor + "." + self._extension
        else:
            file_name_monitor = file_name_monitor + ".raw." + str(page).rjust(4, '0') + "." + extension

        return path + "/" + file_name_monitor

    def compare_query(self, query_1, query_2, index=0):
        return super().compare_query(query_1, query_2)


class CacheFiltered(Cache):
    def __init__(self, root_path, date_ini, date_end, query_name, a_query):
        super().__init__(root_path, date_ini, date_end, query_name, a_query)

    def make_path(self, page=None, extension=_default_extension):
        path = self._make_path_root() + "/" + str(self._a_id).replace(".", "/")

        file_name_monitor = self._date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            self._date_end.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            str(self._a_id)

        file_name_monitor = file_name_monitor + "." + extension

        return path + "/" + file_name_monitor

    def compare_query(self, query_1, query_2, index=0):
        equal = super().compare_query(query_1, query_2)

        for index in range(0, len(query_1["query"])):
            equal = equal and (query_1["query"][index]["epsilon"] == query_2["query"][index]["epsilon"])

        return equal


class CacheSummaryHourly(Cache):
    def __init__(self, root_path, date_ini, date_end, query_name, a_query):
        super().__init__(root_path, date_ini, date_end, query_name, a_query)

    def make_path(self, page=None, extension='cvs.gz'):
        path = self._make_path_root() + "/summary"

        file_name_monitor = self._date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            self._date_end.strftime("%Y-%m-%d_%H_%M_%S")

        file_name_monitor = file_name_monitor + "." + self._query_name + "." + "summary" + "." + extension

        return path + "/" + file_name_monitor

    def compare_query(self, query_1, query_2, index=0):
        equal = super().compare_query(query_1, query_2)

        if len(query_1["query"]) > 0:
            equal = equal and (query_1["query"][index]["epsilon"] == query_2["query"][index]["epsilon"])

        return equal


class CacheSummary(Cache):
    def __init__(self, root_path, date_ini, date_end, query_name, a_query):
        super().__init__(root_path, date_ini, date_end, query_name, a_query)

    def make_path(self, page=None, extension='cvs.gz'):
        path = self._root_path + "/summary/" + self._query_name

        file_name_monitor = self._date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            self._date_end.strftime("%Y-%m-%d_%H_%M_%S")

        file_name_monitor = file_name_monitor + "." + self._query_name + "." + "summary" + "." + extension

        return path + "/" + file_name_monitor

    def compare_query(self, query_1, query_2, index=0):
        equal = super().compare_query(query_1, query_2)

        if len(query_1["query"]) > 0:
            equal = equal and (query_1["query"][index]["epsilon"] == query_2["query"][index]["epsilon"])

        return equal
