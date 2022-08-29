import json
import os
from os.path import exists

import pandas as pd


def _make_path_root(root_path, date_ini, date_end, a_id):
    path = root_path + "/" + \
           date_ini.strftime("%Y-%m-%d") + "/" + date_ini.strftime("%H") + \
           "/" + str(a_id).replace(".", "/")

    return path


def make_path_raw(root_path, date_ini, date_end, a_id, page=None, extension='gz'):
    path = _make_path_root(root_path, date_ini, date_end, a_id) + "/raw"

    file_name_monitor = str(a_id) + \
                        "." + \
                        date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                        "." + \
                        date_end.strftime("%Y-%m-%d_%H_%M_%S")
    if page is None:
        file_name_monitor = file_name_monitor + "." + extension
    else:
        file_name_monitor = file_name_monitor + ".raw." + str(page).rjust(4, '0') + "." + extension

    return path + "/" + file_name_monitor


def make_path_filtered(root_path, date_ini, date_end, a_id, extension='gz'):
    path = _make_path_root(root_path, date_ini, date_end, a_id)

    file_name_monitor = str(a_id) + \
                        "." + \
                        date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                        "." + \
                        date_end.strftime("%Y-%m-%d_%H_%M_%S")
    file_name_monitor = file_name_monitor + "." + extension

    return path + "/" + file_name_monitor


def make_path_summary_hourly(root_path, date_ini, date_end, query_name, extension='gz'):
    path = root_path + "/" \
           + date_ini.strftime("%Y-%m-%d") + "/" + date_ini.strftime("%H") \
           + "/summary"

    file_name_monitor = date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                        "." + \
                        date_end.strftime("%Y-%m-%d_%H_%M_%S")

    file_name_monitor = query_name + "." + "merge" + "." + file_name_monitor + "." + extension

    return path + "/" + file_name_monitor


def make_path_summary(root_path, date_ini, date_end, query_name, extension='gz'):
    path = root_path + "/summary/" + query_name

    file_name_monitor = date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                        "." + \
                        date_end.strftime("%Y-%m-%d_%H_%M_%S")
    file_name_monitor = file_name_monitor + ".summary" + "." + extension

    return path + "/" + file_name_monitor


def store_query(file_name, date_ini, date_end, query_name, a_query):
    query = {
        "date_ini": date_ini.strftime("%Y-%m-%d %H_%M_%S"),
        "date_end": date_end.strftime("%Y-%m-%d %H_%M_%S"),
        "query_name": query_name,
        "query": a_query
    }
    if not exists(os.path.dirname(file_name)):
        os.makedirs(os.path.dirname(file_name))
    with open(file_name, 'w') as outfile:
        outfile.write(json.dumps(query))


def store_query_raw(root_path, date_ini, date_end, query_name, a_query, index=0, page=''):
    _id = a_query[index]['component'] + "." + a_query[index]['monitor']
    file_name = make_path_raw(root_path, date_ini, date_end, _id, page, extension='json')

    store_query(file_name, date_ini, date_end, query_name, a_query)


def store_query_filtered(root_path, date_ini, date_end, query_name, a_query, index=0):
    _id = a_query[index]['component'] + "." + a_query[index]['monitor']
    file_name = make_path_filtered(root_path, date_ini, date_end, _id, extension='json')

    store_query(file_name, date_ini, date_end, query_name, a_query)


def store_query_summary_hourly(root_path, date_ini, date_end, query_name, a_query, page=''):
    file_name = make_path_summary_hourly(root_path, date_ini, date_end, query_name, extension='json')

    store_query(file_name, date_ini, date_end, query_name, a_query)


def store_query_summary(root_path, date_ini, date_end, query_name, a_query, index=0, page=''):
    _id = a_query[index]['component'] + "." + a_query[index]['monitor']
    file_name = make_path_summary(root_path, date_ini, date_end, query_name, extension='json')

    store_query(file_name, date_ini, date_end, query_name, a_query)


def read_query_raw(root_path, date_ini, date_end, a_query, index=0, page=''):
    _id = a_query[index]['component'] + "." + a_query[index]['monitor']
    file_name = make_path_raw(root_path, date_ini, date_end, _id, page, extension='json')

    with open(file_name) as json_file:
        return json.load(json_file)


def read_query_filtered(root_path, date_ini, date_end, a_query, index=0):
    _id = a_query[index]['component'] + "." + a_query[index]['monitor']
    file_name = make_path_filtered(root_path, date_ini, date_end, _id, extension='json')

    with open(file_name) as json_file:
        return json.load(json_file)


def read_query_hourly(root_path, date_ini, date_end, query_name, index=0):
    file_name = make_path_summary_hourly(root_path, date_ini, date_end, query_name, extension='json')

    with open(file_name) as json_file:
        return json.load(json_file)


def read_query_summary(root_path, date_ini, date_end, query_name):
    file_name = make_path_summary(root_path, date_ini, date_end, query_name, extension='json')

    with open(file_name) as json_file:
        return json.load(json_file)


def is_data_frame_raw_cached(root_path, date_ini, date_end, monitor_id, query_name, a_query, page=''):
    file_name_cached = make_path_raw(root_path, date_ini, date_end, monitor_id, page, extension='json')

    if not exists(file_name_cached):
        return False

    query_build = build_query(date_ini, date_end, query_name, a_query, page)
    query_disk = read_query_raw(root_path, date_ini, date_end, a_query, page)

    return compare_raw_query(query_build, query_disk)


def is_data_frame_filtered_cached(root_path, date_ini, date_end, monitor_id, query_name, a_query):
    file_name_cached = make_path_filtered(root_path, date_ini, date_end, monitor_id, extension='json')

    if not exists(file_name_cached):
        return False

    query_build = build_query(date_ini, date_end, query_name, a_query)
    query_disk = read_query_filtered(root_path, date_ini, date_end, a_query)

    return compare_filtered_query(query_build, query_disk)


def is_data_frame_summary_hourly_cached(root_path, date_ini, date_end, query_name, a_query):
    file_name_cached = make_path_summary_hourly(root_path, date_ini, date_end, query_name, extension='json')

    if not exists(file_name_cached):
        return False

    query_build = build_query(date_ini, date_end, query_name, a_query)
    query_disk = read_query_hourly(root_path, date_ini, date_end, query_name)

    return compare_summary_query(query_build, query_disk)


def is_data_frame_summary_cached(root_path, date_ini, date_end, query_name, a_query):
    file_name_cached = make_path_summary(root_path, date_ini, date_end, query_name, extension='json')

    if not exists(file_name_cached):
        return False

    query_build = build_query(date_ini, date_end, query_name, a_query)
    query_disk = read_query_summary(root_path, date_ini, date_end, query_name)

    return compare_summary_query(query_build, query_disk)


def data_frame_raw_from_cache(root_path, date_ini, date_end, monitor_id, page=''):
    file_name = make_path_raw(root_path, date_ini, date_end, monitor_id, page)
    return pd.read_csv(file_name, compression='infer')


def data_frame_filtered_from_cache(root_path, date_ini, date_end, monitor_id):
    file_name = make_path_filtered(root_path, date_ini, date_end, monitor_id)
    return pd.read_csv(file_name, compression='infer')


def data_frame_summary_hourly_from_cache(root_path, date_ini, date_end, query_name):
    file_name = make_path_summary_hourly(root_path, date_ini, date_end, query_name)
    return pd.read_csv(file_name, compression='infer')


def data_frame_summary_from_cache(root_path, date_ini, date_end, query_name):
    file_name = make_path_summary(root_path, date_ini, date_end, query_name)
    return pd.read_csv(file_name, compression='infer')


def data_frame_raw_to_cache(data_frame, root_path, date_ini, date_end, monitor_id, query_name, q_entry, page=''):
    file_name = make_path_raw(root_path, date_ini, date_end, monitor_id, page)
    if not exists(os.path.dirname(file_name)):
        os.makedirs(os.path.dirname(file_name))

    data_frame.to_csv(file_name, index=False, compression='infer')
    store_query_raw(root_path, date_ini, date_end, query_name, q_entry, page)


def data_frame_filtered_to_cache(data_frame, root_path, date_ini, date_end, monitor_id, query_name, q_entry):
    file_name = make_path_filtered(root_path, date_ini, date_end, monitor_id)
    if not exists(os.path.dirname(file_name)):
        os.makedirs(os.path.dirname(file_name))

    data_frame.to_csv(file_name, index=False, compression='infer')
    store_query_filtered(root_path, date_ini, date_end, query_name, q_entry)


def data_frame_summary_hourly_to_cache(data_frame, root_path, date_ini, date_end, query_name, a_query):
    file_name = make_path_summary_hourly(root_path, date_ini, date_end, query_name)
    if not exists(os.path.dirname(file_name)):
        os.makedirs(os.path.dirname(file_name))

    data_frame.to_csv(file_name, index=False, compression='infer')
    store_query_summary_hourly(root_path, date_ini, date_end, query_name, a_query)


def data_frame_summary_to_cache(data_frame, root_path, date_ini, date_end, query_name, a_query):
    file_name = make_path_summary(root_path, date_ini, date_end, query_name)
    if not exists(os.path.dirname(file_name)):
        os.makedirs(os.path.dirname(file_name))

    data_frame.to_csv(file_name, index=False, compression='infer')
    store_query_summary(root_path, date_ini, date_end, query_name, a_query)


def read_query(path, page=''):
    with open(path + "/query" + str(page) + ".json") as json_file:
        return json.load(json_file)


def build_query(date_ini, date_end, query_name, query, page=None):
    q = {
        "date_ini": date_ini.strftime("%Y-%m-%d %H_%M_%S"),
        "date_end": date_end.strftime("%Y-%m-%d %H_%M_%S"),
        "query_name": query_name,
        "query": query
    }

    if page is not None:
        q["page"] = page

    return q


def compare_raw_query(q1, q2, index=0):
    equal = True

    equal = equal and (q1["date_ini"] == q2["date_ini"])
    equal = equal and (q1["date_end"] == q2["date_end"])
    if len(q1["query"]) > 0:
        equal = equal and (q1["query"][index]["component"] == q2["query"][index]["component"])
        equal = equal and (q1["query"][index]["monitor"] == q2["query"][index]["monitor"])
        equal = equal and (q1["query"][index]["type"] == q2["query"][index]["type"])

    #    if q1["page"] and q2["page"]:
    #        equal = equal and (q1["page"] == q2["page"])

    return equal


def compare_filtered_query(q1, q2, index=0):
    equal = compare_raw_query(q1, q2, index)

    if len(q1["query"]) > 0:
        equal = equal and (q1["query"][index]["epsilon"] == q2["query"][index]["epsilon"])

    return equal


def compare_summary_query(q1, q2):
    equal = len(q1["query"]) == len(q2["query"])
    for index in range(0, len(q1["query"])):
        equal = equal and compare_filtered_query(q1, q2, index)

    return equal


class CacheRaw(object):
    def __init__(self, root_path, date_ini, date_end, a_id, query_name, a_query):
        self._date_ini = date_ini
        self._date_end = date_end
        self._a_id = a_id
        self._root_path = _make_path_root(root_path)
        self._query_name = query_name
        self._a_query = a_query

    def _make_path_root(self, root_path):
        path = root_path + "/" + \
               self._date_ini.strftime("%Y-%m-%d") + "/" + self._date_ini.strftime("%H") + \
               "/" + str(id).replace(".", "/")
        return path
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

    def make_path(self, page=None):
        path = self._root_path + "/raw"

        file_name_monitor = str(self._a_id) + \
                            "." + \
                            self._date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                            "." + \
                            self._date_end.strftime("%Y-%m-%d_%H_%M_%S")
        if page is None:
            file_name_monitor = file_name_monitor + "." + self._extension
        else:
            file_name_monitor = file_name_monitor + ".raw." + str(page).rjust(4, '0') + "." + self._extension #todo

        return path + "/" + file_name_monitor

    def store_query(self, page=''):
        file_name = self._make_path(page)
        self._store_query(file_name)

    def read_query(self, root_path, date_ini, date_end, a_query, index=0, page=''):
        _id = a_query[index]['component'] + "." + a_query[index]['monitor']
        file_name = make_path_raw(root_path, date_ini, date_end, _id, page, extension='json')

        with open(file_name) as json_file:
            return json.load(json_file)
