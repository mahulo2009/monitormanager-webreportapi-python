import argparse
import json
import logging
import sys

_DEFAULT_HOST_NAME = "calp-vwebrepo"
_DEFAULT_HOST_PORT = "8081"

from mmwebreport.retrieve.daterange import *
from mmwebreport.retrieve.retrieve import RetrieveMonitor

def Main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--query_file',
                        required=True,
                        type=str,
                        default="query.json",
                        dest="query_file",
                        help="Query configuration file")
    parser.add_argument('--host',
                        required=False,
                        type=str,
                        default=_DEFAULT_HOST_NAME,
                        dest="host_name",
                        metavar="<monitor manager web report server host>",
                        help="Monitor Manager Web Report Server host name")
    parser.add_argument('-p', '--port',
                        required=False,
                        type=str,
                        default=_DEFAULT_HOST_PORT,
                        dest="port_name",
                        help="Monitor Manager Web Report Server port number")
    parser.add_argument('-c', '--clean_cache',
                        required=False,
                        type=bool,
                        default=False,
                        dest="clean_cache",
                        help="Clean cache of previous Monitor Manager Reports")

    args = parser.parse_args()

    with open(args.query_file) as json_file:
        json_query = json.load(json_file)

    query_name = json_query["query_config"]["query_name"]
    query_fill_forwarder = json_query["query_config"]["query_fill_forwarder"]

    query_monitor_list = json_query["query_monitor_list"]

    retrieve = RetrieveMonitor(_DEFAULT_HOST_NAME, _DEFAULT_HOST_PORT,
                               query_monitor_list,
                               query_name,
                               q_clean_cache=args.clean_cache,
                               q_fillfw=query_fill_forwarder)

    date_range_list = json_query["query_date_range"]

    DateRange = getattr(sys.modules[__name__], date_range_list["type"])
    date_range = DateRange()
    date_range.from_json(date_range_list)

    data_frame = retrieve.retrieve_summary(date_range)

    data_frame.to_csv(query_name + ".csv")

if __name__ == "__main__":
    Main()
