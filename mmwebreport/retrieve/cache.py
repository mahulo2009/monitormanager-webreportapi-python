import os
import json

from datetime import datetime, time, timedelta


def make_path_raw(root_path, date_ini, date_end, a_id, page=None):
    path = root_path + "/" + \
           date_ini.strftime("%Y-%m-%d") + "/" + date_ini.strftime("%H") + \
           "/" + str(a_id).replace(".", "/") + "/raw"

    file_name_monitor = str(a_id) + \
                        "." + \
                        date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                        "." + \
                        date_end.strftime("%Y-%m-%d_%H_%M_%S")
    if page is None:
        file_name_monitor = file_name_monitor + ".gz"
    else:
        file_name_monitor = file_name_monitor + ".raw." + str(page).rjust(4, '0') + ".gz"

    return path + "/" + file_name_monitor


def make_path_filtered(root_path, date_ini, date_end, a_id):
    path = root_path + "/" + \
           date_ini.strftime("%Y-%m-%d") + "/" + date_ini.strftime("%H") + \
           "/" + str(a_id).replace(".", "/")

    file_name_monitor = str(a_id) + \
                        "." + \
                        date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                        "." + \
                        date_end.strftime("%Y-%m-%d_%H_%M_%S")
    file_name_monitor = file_name_monitor + ".gz"

    return path + "/" + file_name_monitor


def make_path_summary_hourly(root_path, date_ini, date_end, query_name):
    path = root_path + "/" \
           + date_ini.strftime("%Y-%m-%d") + "/" + date_ini.strftime("%H") \
           + "/summary"

    file_name_monitor = date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                        "." + \
                        date_end.strftime("%Y-%m-%d_%H_%M_%S")

    file_name_monitor = query_name + "." + "merge" + "." + file_name_monitor + ".gz"

    return path + "/" + file_name_monitor


def store_query_summary_hourly(root_path, date_ini, date_end, query_name, query):
    query = {
        "date_ini": date_ini.strftime("%Y-%m-%d %H_%M_%S"),
        "date_end": date_end.strftime("%Y-%m-%d %H_%M_%S"),
        "query_name": query_name,
        "query": query
    }

    with open(root_path + '/query.json', 'w') as outfile:
        outfile.write(json.dumps(query))


def make_path_summary_query(root_path, date_ini, date_end, query_name):
    path = root_path + "/summary/" + query_name

    file_name_monitor = date_ini.strftime("%Y-%m-%d_%H_%M_%S") + \
                        "." + \
                        date_end.strftime("%Y-%m-%d_%H_%M_%S")
    file_name_monitor = file_name_monitor + ".summary" + ".gz"

    return path + "/" + file_name_monitor


def store_query(root_path, date_ini, date_end, query_name, query, page=''):
    query = {
        "date_ini": date_ini.strftime("%Y-%m-%d %H_%M_%S"),
        "date_end": date_end.strftime("%Y-%m-%d %H_%M_%S"),
        "query_name": query_name,
        "query": query
    }

    if page is not None:
        query["page"] = page

    with open(root_path + '/query'+str(page)+'.json', 'w') as outfile:
        outfile.write(json.dumps(query))


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


def read_query(path, page=''):
    with open(path + "/query" + str(page) + ".json") as json_file:
        return json.load(json_file)


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
