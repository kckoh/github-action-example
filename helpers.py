import re
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional


def jnInserter(jn, dropQ=False, dropNull=False):
    if isinstance(jn, list):
        if dropQ:
            if dropNull:
                jn = [x for x in jn if x is not None]
            jn = [re.sub(r"(?<!\\)'", r"\'", x) for x in jn]
            jn = [x.replace(r"%", r"'||CHR(38)||'") for x in jn]
        rjn = "'" + "', '".join(jn) + "'"
    else:
        if isinstance(jn, str):
            if dropQ:
                jn = re.sub(r"(?<!\\)'", r"\'", jn)
        rjn = f"'{jn}'"
    return rjn


def getMetricAndParamClasses(api_input, metric_dict, job_metric_dict, hash_dict):
    metric = api_input["metric"]
    job = ""
    if "job" in api_input.keys():
        job = hash_dict[api_input["job"]]
    param_class = metric_dict[metric]["param_class"]
    metric_class = metric_dict[metric]["metric_class"]
    if len(str(job)) > 0:
        if job in job_metric_dict.keys():
            if metric in job_metric_dict[job]:
                if "param_class" in job_metric_dict[job][metric].keys():
                    param_class = job_metric_dict[job][metric]["param_class"]
                if "metric_class" in job_metric_dict[job][metric].keys():
                    metric_class = job_metric_dict[job][metric]["metric_class"]
    return (param_class, metric_class)


def getParameterFromJMD(param, params, jmd=None):
    if jmd is None:
        from dictionaries import job_metric_dict

        jmd = job_metric_dict
    metric = params.metric
    job = params.job
    if job in jmd.keys():
        if metric in jmd[job].keys():
            if param in jmd[job][metric].keys():
                return jmd[job][metric][param]
    return ""


def get_parameter_from_md_or_jmd(param, params, md=None, jmd=None):
    # First, attempt to fetch from the md (metric_dict)
    if md is None:
        from dictionaries import metric_dict

        md = metric_dict

    metric = params.metric
    if metric in md.keys():
        if param in md[metric].keys():
            return md[metric][param]

    # If not found in md, then attempt to fetch from jmd (job_metric_dict)
    if jmd is None:
        from dictionaries import job_metric_dict

        jmd = job_metric_dict

    job = params.job
    if job in jmd.keys():
        if metric in jmd[job].keys():
            if param in jmd[job][metric].keys():
                return jmd[job][metric][param]

    return ""


def makeMultiDBQuery(
    query,
    start_sql="",
    union_sql="            UNION ALL",
    end_sql="        ;",
    country_dict=None,
):
    if country_dict is None:
        country_dict = {  # zendesk_dict
            "AT": {
                "suffix": "_1crm_at",
                "country_tag": "AT",
                "country_name": "Austria",
            },
            "DE": {
                "suffix": "_1crm_de",
                "country_tag": "DE",
                "country_name": "Germany",
            },
            #     "UK": { "suffix": "_1crm_uk",
            #             "country_tag": 'UK',
            #             "country_name": 'United Kingdom'},
            #     "US": { "suffix": "_1crm_us",
            #             "country_tag": 'US',
            #             "country_name": 'United States'},
            "FR": {"suffix": "_1crm_fr", "country_tag": "FR", "country_name": "France"},
            #     "GL": { "suffix": "_1crm_gl",
            #             "country_tag": 'GL',
            #             "country_name": 'Global'},
            #     "NL": { "suffix": "_1crm_nl",
            #             "country_tag": 'NL',
            #             "country_name": 'Not Listed'}
        }
    sql = start_sql
    for i, x in enumerate(country_dict.keys()):
        formatted_query = query.format(
            suffix=country_dict[x]["suffix"],
            country_name=country_dict[x]["country_name"],
        )
        if i > 0:
            sql = sql + union_sql
        sql = sql + formatted_query
    sql = sql + end_sql
    # print(sql)
    return sql


def get_db(job, jmdct):
    db = "legacy_uk"
    if job in jmdct.keys():
        if "weather_locations" in jmdct[job]:
            if "db" in jmdct[job]["weather_locations"].keys():
                db = jmdct[job]["weather_locations"]["db"]
    return db


def findNth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start + len(needle))
        n -= 1
    return start


# From https://stackoverflow.com/questions/579310/formatting-long-numbers-as-strings-in-python/45846841
def human_format(num, dec=2):
    if isinstance(num, (int, float)):
        fnum = float("{:.3f}".format(num))
    else:
        print(f"human_format() can't turn num: {num}, type: {type(num)} into a float.")
        return str(num)
    magnitude = 0
    while abs(fnum) >= 1000:
        magnitude += 1
        fnum /= 1000.0
    if magnitude == 0 and isinstance(num, int):
        return str(num)
    return f"{round(fnum, dec)}{['', 'K', 'M', 'B', 'T'][magnitude]}"


valid_hierarchies = {
    "hierarchy1": "product_type",
    "hierarchy2": "location",
    "hierarchy3": "region",
    "hierarchy4": "project",
    "hierarchy5": "manager",
    "hierarchy6": "client",
    "hierarchy7": "project_id",
}  # only used as list


def x_months_ago(x, frmt="%Y-%m-%d"):
    return (datetime.utcnow() - pd.DateOffset(months=x)).replace(day=1).strftime(frmt)


def format_string_with_dict(
    string_input: str,
    defaults: "dict[str, str]",
    replacements: "Optional[dict[str, str]]" = None,
) -> str:
    if replacements is None:
        replacements = {}
    if defaults is None:
        defaults = {}

    # Create a new dictionary by updating the defaults with the replacements
    merged_dict = defaults.copy()
    merged_dict.update(replacements)
    print(string_input)
    print(merged_dict)
    return string_input.format(**merged_dict)


def get_last_saturday(from_date=datetime.now()):
    days_since_saturday = (from_date.weekday() - 5) % 7
    if days_since_saturday == 0:
        days_since_saturday = 7
    last_saturday = from_date - timedelta(days=days_since_saturday)
    return last_saturday.strftime("%Y-%m-%d")


def get_previous_date(input_date_str: str, date_format: str = "%Y-%m-%d") -> str:
    input_date = datetime.strptime(input_date_str, date_format).date()
    previous_date = input_date - timedelta(days=1)
    previous_date_str = previous_date.strftime(date_format)
    return previous_date_str


def day_before(date_str: str) -> str:
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    day_before_obj = date_obj - timedelta(days=1)
    day_before_date_str = day_before_obj.strftime("%Y-%m-%d")
    return day_before_date_str


def last_valid_week_starting_saturday(most_recent_date):
    date_obj = datetime.strptime(most_recent_date, "%Y-%m-%d")
    current_date = datetime.today()

    # Calculate most recent Saturday
    if date_obj.weekday() == 6:  # Sunday
        most_recent_saturday = date_obj - timedelta(days=1)
    elif date_obj.weekday() == 5:  # Saturday
        most_recent_saturday = date_obj
    else:
        most_recent_saturday = date_obj - timedelta(days=date_obj.weekday() + 2)

    # Calculate end date (following Friday or today if sooner)
    end_date = min(most_recent_saturday + timedelta(days=6), current_date)

    print(f"Most recent Saturday: {most_recent_saturday.strftime('%Y-%m-%d')}")
    print(f"End date: {end_date.strftime('%Y-%m-%d')}")
    return most_recent_saturday.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
