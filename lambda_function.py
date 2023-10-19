# API parameters:
# Required:
# metric
# specifies what data to look at & how to process it.
### Could sometimes break how to process off as another parameter?
# Optional:
# layer
# '1', L1, default if layer not specified
# '2', L2
# 'filters' returns valid filter-options; requires 'filter' parameter,
#           returns valid options for that filter given the
#           startDate, endDate and 'filters' parameters.
# 'groupings' returns valid dates & grouping-names;
#           if 'filter' parameter passed,
#           returns valid filter-options for that filter
# 'csv' gives .csv-formatted data dump
#       given rest of (L2-compatible) parameters passed.

# filters
# json parsed as Python-dictionary with grouping-names as keys and
# an array of options to include.
# (If the grouping is hierarchical, node_id values may not be lowest-level.)
# filter
# either 'dates' or a valid grouping-name;
# specifies which filter's options to return for layer:'groupings'
# startDate
# 1st date to include; default otherwise inferred from metric/ layer/ data
# endDate
# last date to include; default otherwise inferred from metric/ layer/ data
# job
# narrows TF-based metrics to particular form_id values,
# narrows CRM-based metrics to particular jobs.number / jobs.id values.
# by
# If specified, data returned grouped by 'by' parameter.

###############
### Imports ###
###############

import json
import sqlalchemy as sa
from time import perf_counter
from urllib.parse import unquote

from impactlib import rs_connect
from dictionaries import hash_dict, metric_dict, job_metric_dict
from helpers import getMetricAndParamClasses

####################
### /end Imports ###
####################

######################
### lambda_handler ###
######################


def lambda_handler(event, context):
    tic = perf_counter()
    api_input = {}
    if event is not None:
        if "queryStringParameters" in event.keys():
            api_input = event["queryStringParameters"]
        else:
            api_input = event

    # Just for local testing
    local_flag = api_input.get("local_flag", False)
    print("Got local flag; about to try rs_connect().")
    if local_flag:
        engine, server = rs_connect()
    else:
        engine = rs_connect()
    print("Got engine.")
    with engine.connect() as conn:
        conn.execute("SET enable_result_cache_for_session TO OFF;")
    ###################
    # Run stuff here. #
    ###################
    param_class, metric_class = getMetricAndParamClasses(
        api_input, metric_dict, job_metric_dict, hash_dict
    )
    print(param_class)
    metric_parameters = param_class(
        engine, api_input, hash_dict, metric_dict, job_metric_dict
    )
    metric = metric_class(engine, metric_parameters, metric_dict, job_metric_dict)
    metric.useMethod()
    metric.setReturnJson()
    ###################
    # End stuff here. #
    ###################

    engine.dispose()
    # Just for local testing
    if local_flag:
        server.stop()
    toc = perf_counter()
    print(f"lambda_hander() took {toc - tic:0.4f} seconds.\n")
    return {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": (
                "Content-Type,X-Amz-Date,Authorization,"
                "X-Api-Key,X-Amz-Security-Token"
            ),
            "Access-Control-Allow-Methods": "GET,OPTIONS",
        },
        "body": json.dumps(metric.return_json),
    }


def use_query_string(qs):
    params = qs.split("&amp;")
    api_input = {"local_flag": True}
    for param in params:
        key_val = param.split("=")
        api_input[key_val[0]] = unquote(key_val[1])
    print(api_input)
    return api_input


if __name__ == "__main__":
    metrics = ["non_activated_stores"]
    layers = ["1", "2", "groupings", "filters", "csv"]
    layers = ["2"]
    for i in metrics:
        for j in layers:
            api_input = {
                "job": "37ab50b929ccd6166b954a3cf38e57c6a5a6867a4ca2a78cc4637f3d0864ade6",
                "layer": j,
                "metric": i,
                # "by": "day_of_week",
                # "filters": "%7B%22location%22:%22111\t5154%22%7D",
                # 'filter': 'day_of_week',
                "startDate": "2023-09-23",
                # 'endDate': '2023-01-25',
                "local_flag": True,
            }
            print()
            print(f"api_input: {api_input}")
            print(lambda_handler(api_input, None))
    # api_input = {
    #     'local_flag': True, 'layer': '1', 'metric': 'daily_transaction',
    #     'job': '0f738e1d447a2274a4be94c72168c44a8104cef75730932a2e62f606de65a8df'
    # }
    # print(lambda_handler(api_input, None))
    # qs = (
    #     "metric=reported_issues_xfctr&amp;job=37ab50b929ccd6166b954a3cf38e57c6a5a6867a4ca2a78cc4637f3d0864ade6&amp;layer=2&amp;startDate=2023-09-23&amp;endDate=2023-09-24&amp;filters=%7B%22hierarchy2%22:%221002%22%7D&amp;by=hierarchy2&amp;locale=us"
    #     # "metric=daily_transaction&amp;job=37ab50b929ccd6166b954a3cf38e57c6a5a6867a4ca2a78cc4637f3d0864ade6&amp;layer=1&amp;locale=us"
    #     # "metric=reported_issues_xfctr&amp;"
    #     # "job=37ab50b929ccd6166b954a3cf38e57c6a5a6867a4ca2a78cc4637f3d0864ade6&amp;"
    #     # "layer=2&amp;startDate=2023-09-23&amp;endDate=2023-09-25&amp;"
    #     # "by=day_of_week&amp;locale=us"
    # )
    # print(lambda_handler(use_query_string(qs), None))
