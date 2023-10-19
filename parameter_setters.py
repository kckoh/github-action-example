"""Classes to establish what the paramters are for metric_generators classes."""
from abc import ABC, abstractmethod
import pandas as pd
from urllib.parse import unquote  # , quote
import json
from copy import deepcopy
from datetime import datetime, timedelta
from sqlalchemy.engine import Engine

from helpers import (
    jnInserter,
    getParameterFromJMD,
    makeMultiDBQuery,
    get_db,
    valid_hierarchies,
    x_months_ago,
    last_valid_week_starting_saturday,  # get_last_saturday,
    day_before,
    get_parameter_from_md_or_jmd,
)
from typing import Dict, Any, List

###################################
### Parameter Gathering Classes ###
###################################


class Params(ABC):
    """Object containing parameters needed to generate correct DB query."""

    __slots__ = [
        "metric",
        "field_ref",
        "ai",
        "filters",
        "filter",
        "layer",
        "grouping_inclusions",
        "by",
        "sby",
        "job",
        "startDate",
        "endDate",
        "target",
        "target_cte",
        "target_join",
        "filters_string",
        "groupings_dict",
        "filters_dict",
        "by_dict",
        "job_filter",
        "hfd",
        "l1_ref",
        "l1_refs",
        "hash_dict",
        "debug",
        "truncate",
    ]
    # Check whether obsolete: "format"

    def __init__(self, engine, api_input, hash_dict, metric_dict, job_metric_dict):
        self.setStandardParameters(
            api_input, engine, hash_dict, metric_dict, job_metric_dict
        )
        self.setFieldRef(metric_dict, job_metric_dict)
        self.setTargetSQL(job_metric_dict)
        self.setJobFilter(job_metric_dict)
        self.setFiltersString()
        self.set_dates(engine, job_metric_dict)

    def setFieldRef(
        self, metric_dict: Dict[str, Dict], job_metric_dict: Dict[str, Dict]
    ) -> None:
        # Default to self.metric as field_ref
        self.field_ref = self.metric

        # Check if metric_dict has a field_ref for self.metric
        if self.metric in metric_dict and "field_ref" in metric_dict[self.metric]:
            self.field_ref = metric_dict[self.metric]["field_ref"]

        # Check if job_metric_dict has a field_ref for self.metric and job
        if self.job in job_metric_dict and self.metric in job_metric_dict[self.job]:
            if "field_ref" in job_metric_dict[self.job][self.metric]:
                self.field_ref = job_metric_dict[self.job][self.metric]["field_ref"]

    def setStandardParameters(
        self,
        api_input: Dict[str, Any],
        engine: Engine,
        hash_dict: Dict[str, str],
        md: Dict[str, Dict],
        jmd: Dict[str, Dict],
    ) -> None:
        # Create a shallow copy of the input dictionary to avoid modifying the original
        self.ai = api_input.copy()
        # Set metric, filters, hfd, layer, grouping_inclusions, by, and sby
        # instance variables
        self.metric = self.ai.get("metric")
        self.filters = {}
        self.hfd = {}
        self.layer = self.ai.get("layer", "1")
        self.grouping_inclusions = set()
        self.by = ""
        self.sby = ""
        self.debug = self.ai.get("local_flag", False)
        # Process the 'by' field if it exists in the input dictionary
        if "by" in self.ai:
            if self.ai["by"] != "all":
                by = self.ai["by"]
                self.sby = by
                self.grouping_inclusions.add(by)
                self.by = f", {by}"

        # Process the 'filters' field if it exists in the input dictionary
        if "filters" in self.ai:
            filters = unquote(self.ai["filters"])
            filters = json.loads(filters, strict=False)
            for f, v in filters.items():
                # Ignore the filter if the list of values is empty
                if v == []:
                    raise Exception(f"No values included from {f}.")
                else:
                    # Add the filter to the grouping_inclusions set
                    # We want filters added to 'groupings'_dict
                    # because it is used to add text that prepares for both
                    # grouping and filtering
                    self.grouping_inclusions.add(f)

                    # If the filter is a valid hierarchy, add its corresponding
                    # hierarchy to the grouping_inclusions set
                    if f in valid_hierarchies:
                        self.grouping_inclusions.add(valid_hierarchies[f])
                        self.handleHierarchy(f, v, engine)
                    else:
                        # Otherwise, add the filter to the filters dictionary
                        self.filters[f] = v
        # Set the filter instance variable
        self.filter = self.ai.get("filter")

        if (
            self.layer == "1"
            and "job" in self.ai.keys()
            and "indicator_class" in jmd[hash_dict[self.ai["job"]]][self.metric]
            and str(jmd[hash_dict[self.ai["job"]]][self.metric]["indicator_class"])
            == "<class 'indicators.IndicatorVsPrevMonth'>"
        ):
            self.grouping_inclusions.add("month")
            self.by = ", month"

        if (
            self.layer == "1"
            and "job" in self.ai.keys()
            and "indicator_class" in jmd[hash_dict[self.ai["job"]]][self.metric]
            and str(jmd[hash_dict[self.ai["job"]]][self.metric]["indicator_class"])
            == "<class 'indicators.IndicatorVsPrevQuarter'>"
        ):
            self.grouping_inclusions.add("quarter")
            self.by = ", quarter"
        # Set the groupings_dict instance variable
        self.setGroupingsDict()
        # Set the job instance variable based on the 'job' field in the input dictionary
        self.hash_dict = hash_dict
        self.job = self.hash_dict.get(self.ai.get("job", ""), "")

        # Set the l1_ref instance variable using
        # the getParameterFromJMD utility function
        self.l1_ref = get_parameter_from_md_or_jmd("l1_reference", self, md, jmd)
        self.l1_refs = get_parameter_from_md_or_jmd("l1_reference_list", self, md, jmd)

        jmd_truncate = str(getParameterFromJMD("truncate", self, jmd))
        self.truncate = jmd_truncate if len(jmd_truncate) > 0 else False

    def omitInvalidFilters(self, valid_groupings: Dict[str, Dict]) -> None:
        # Reset as part of FindFilters class initialization because
        # we need access to FF' .setValidGroupings() method to reliably
        # determine which filters are valid.
        invalid_filters = []
        for f in self.filters:
            if f not in valid_groupings:
                invalid_filters.append(f)
        for f in invalid_filters:
            del self.filters[f]
        self.setFiltersString()

    def handleHierarchicalGroupingsDict(
        self, groupings_dict: Dict[str, Dict], hierarchies_list: List
    ) -> Dict[str, Dict]:
        """
        This method handles hierarchical groupings in groupings_dict for the given
        hierarchies_list. It replaces placeholders in the SQL statements with actual
        values using valid_hierarchies.
        :param groupings_dict: A dictionary of groupings.
        :param hierarchies_list: A list of hierarchies to handle.
            eg. TF: [1, 2], CRMs: [3, 4, 5]
        :param valid_hierarchies: A dictionary of valid hierarchies.
        :return: The updated groupings_dict.
        """
        for i in hierarchies_list:
            hx = f"hierarchy{i}"
            if hx in self.filters:
                for grouping, sql_insert in groupings_dict[hx].items():
                    middle_insert = ""
                    # self.hfd[f'{hx}_dict']: handleHierarchy()'s grouping_node_dict
                    for key, value in self.hfd[f"{hx}_dict"].items():
                        repkey = key.replace("'", "\\'")
                        h_insert = f"""WHEN {valid_hierarchies[hx]} IN ({jnInserter(
                            value, dropQ=True)}) THEN '{repkey}'"""
                        middle_insert += h_insert
                    sql_insert = sql_insert.format(
                        middle_insert=middle_insert, hx=hx, metric=self.metric
                    )
                    groupings_dict[hx][grouping] = sql_insert
        return groupings_dict

    def handleGroupingsDict(self, groupings_dict: Dict[str, Dict]) -> Dict[str, Dict]:
        """Omit unused filters, groupings, by-groupings from respective dicts."""
        filters_dict = deepcopy(groupings_dict)
        for fltr in filters_dict:
            if fltr not in self.filters:
                filters_dict[fltr] = {key: "" for key in filters_dict[fltr]}
        self.filters_dict = filters_dict
        for grouping in groupings_dict:
            if grouping not in self.grouping_inclusions:
                groupings_dict[grouping] = {key: "" for key in groupings_dict[grouping]}
        self.groupings_dict = groupings_dict
        by_dict = deepcopy(groupings_dict)
        if "by" in self.ai.keys():
            for grouping in by_dict:
                if grouping != self.ai["by"]:
                    by_dict[grouping] = {key: "" for key in by_dict[grouping]}
            self.by_dict = by_dict
        return groupings_dict

    def handleHierarchy(
        self, hierarchy_name: str, check_ids: str, engine: Engine
    ) -> None:
        """
        With hierarchical filters passed as the node_id values of the grouping-nodes
        used, we need to get the node_name values of (all levels of) the sub-nodes
        within those grouping-nodes for filtering and the names of the grouping-nodes
        to use when grouping.
        """

        def getKids(
            engine: Engine, hierarchy_name: str, check_ids: List[int]
        ) -> pd.DataFrame:
            sql = f"""
            SELECT  h.node_name, g.node_name AS desc_name
            FROM (  SELECT node_id, node_name, d AS desc_id
                    FROM hierarchical_groupings h, h.descendants d) h
            JOIN hierarchical_groupings g ON g.node_id = desc_id 
            WHERE hierarchy_id = {hierarchy_name[-1]}
              AND h.node_id IN ({check_ids})
              --AND h.node_id != h.desc_id
            ORDER BY h.node_id
            ;"""
            df = pd.read_sql(sql, engine)
            return df

        df = getKids(engine, hierarchy_name, check_ids)
        grouping_node_dict: Dict[str, List[str]] = {}
        for _, row in df.iterrows():
            if row["node_name"] in grouping_node_dict:
                grouping_node_dict[row["node_name"]].append(row["desc_name"])
            else:
                grouping_node_dict[row["node_name"]] = [row["desc_name"]]

        self.filters[hierarchy_name] = ",".join(list(df["desc_name"]))
        self.hfd[f"{hierarchy_name}_dict"] = grouping_node_dict

    """
    Now have dictionary like
    {'soft drinks and water': [
        'Pepsi',
        'Sanpellegrino',
        'Jimmys',
        'Costa',
        'Jimmys Iced Coffee Original',
        "Jimmy's Iced Coffee Mocha",
        "Jimmy's Dairy Free Oat Iced Coffee",
        'Sanpellegrino Limonata',
        'Pepsi Max Cherry',
        'Pepsi Max Lime',
        'Costa Coffee Latte',
        'Costa Coffee Caramel Latte'
    ], 'Food': ['Waitrose']}
    """

    def ggie(self, param: str, op: str = "select") -> str:
        # Get Grouping If Exists; else return a blank string.
        # Used in metric generators to ensure no non-existant grouping is referenced.
        return self.groupings_dict.get(param, {}).get(op, "")

    def gfie(self, param: str, op: str = "select") -> str:
        # Get Filter If Exists; else return a blank string.
        # Used in filters generators to ensure no non-existant filter is referenced.
        return self.filters_dict.get(param, {}).get(op, "")

    def set_dates_default(self, engine: Engine) -> None:
        # Default setting; uses max for endDate, l1 startDate;
        # uses min for other startDates.
        self.endDate = self.ai.get("endDate", self.getMaxDate(engine))
        self.startDate = self.ai.get(
            "startDate", self.endDate if self.layer == "1" else self.getMinDate(engine)
        )

    def setDatesFullRange(self, engine: Engine) -> None:
        # Uses max for endDate, min for startDate.
        self.endDate = self.ai.get("endDate", self.getMaxDate(engine))
        self.startDate = self.ai.get("startDate", self.getMinDate(engine))

    def set_dates_l1_lastdayminusone_l2_all(self, engine: Engine) -> None:
        # Uses max for endDate; l1 startDate: day before endDate;
        # uses min for other startDates.
        self.endDate = self.ai.get("endDate", self.getMaxDate(engine))
        self.startDate = self.ai.get(
            "startDate",
            day_before(self.endDate) if self.layer == "1" else self.getMinDate(engine),
        )

    def set_dates_l1_last_valid_week_starting_saturday_l2_all(
        self, engine: Engine
    ) -> None:
        # L1: the week starting with the Saturday on or before the most recent valid data.
        # Otherwise uses max for endDate, min for startDates.
        if self.layer == "1":
            self.startDate, self.endDate = last_valid_week_starting_saturday(
                self.getMaxDate(engine)
            )
        else:
            self.setDatesFullRange(engine)

    def set_dates_l1_all_l2_24_full_months(self, engine: Engine) -> None:
        # Uses max for endDate, min for L1 startDate, 24 full months back for others.
        self.endDate = self.ai.get("endDate", self.getMaxDate(engine))
        self.startDate = self.ai.get(
            "startDate",
            self.getMinDate(engine) if self.layer == "1" else x_months_ago(24),
        )

    # def set_dates_l1_last_saturday_l2_all(self, engine: Engine):
    #     self.endDate = self.ai.get("endDate", self.getMaxDate(engine))
    #     self.startDate = self.ai.get(
    #         "startDate",
    #         get_last_saturday() if self.layer == "1" else self.getMinDate(engine),
    #     )

    def get_end_of_current_month(self):
        current_date = datetime.utcnow()
        next_month = current_date.replace(day=28) + timedelta(days=4)
        first_day_next_month = next_month.replace(day=1)
        end_of_current_month = first_day_next_month - timedelta(days=1)
        return end_of_current_month.strftime("%Y-%m-%d")

    def get_tomorrow_date(self):
        tomorrow = datetime.utcnow() + timedelta(days=1)
        tomorrow_str = tomorrow.strftime("%Y-%m-%d")
        return tomorrow_str

    def set_dates_tomorrow_to_l1_eom_l2_all(self, engine):
        self.endDate = self.ai.get(
            "endDate",
            self.get_end_of_current_month()
            if self.layer == "1"
            else self.getMaxDate(engine),
        )
        self.startDate = self.ai.get("startDate", self.get_tomorrow_date())

    def set_dates_l1_x_full_months_enddate(self, engine):
        # Helper function
        if "endDate" in self.ai.keys():
            self.endDate = self.ai["endDate"]
        else:
            if self.layer in ["1"]:
                self.endDate = (
                    datetime.utcnow().replace(day=1) - pd.DateOffset(days=1)
                ).strftime("%Y-%m-%d")
            else:
                self.endDate = self.getMaxDate(engine)

    def set_dates_l1_x_full_month_l2_24_full_months(self, engine, x):
        self.set_dates_l1_x_full_months_enddate(engine)
        self.startDate = self.ai.get(
            "startDate", x_months_ago(x) if self.layer == "1" else x_months_ago(24)
        )

    def set_dates_l1_2_full_months_l2_24_full_months(self, engine):
        self.set_dates_l1_x_full_month_l2_24_full_months(engine, 2)

    def set_dates_l1_1_full_month_l2_24_full_months(self, engine):
        self.set_dates_l1_x_full_month_l2_24_full_months(engine, 1)

    def set_dates_l1_two_full_months_l2_max_twenty_four_full_months(self, engine):
        self.set_dates_l1_x_full_months_enddate(engine)
        self.startDate = self.ai.get(
            "startDate",
            x_months_ago(2)
            if self.layer in ["1"]
            else max(x_months_ago(24), self.getMinDate(engine)),
        )

    def set_dates_l1_x_full_months_l2_all(self, engine, x):
        self.set_dates_l1_x_full_months_enddate(engine)
        self.startDate = self.ai.get(
            "startDate",
            (datetime.utcnow().replace(day=1) - pd.DateOffset(months=x)).strftime(
                "%Y-%m-%d"
            )
            if self.layer == "1"
            else self.getMinDate(engine),
        )

    def set_dates_l1_2_full_months_l2_all(self, engine):
        self.set_dates_l1_x_full_months_l2_all(engine, 2)

    # def set_dates_l1_x_months_l2_all(self, engine, x):
    #     self.set_dates_l1_x_full_months_enddate(engine)
    #     self.startDate = self.ai.get('startDate',
    #         (datetime.utcnow().replace(day=1) - pd.DateOffset(months=x)).\
    #             strftime("%Y-%m-%d") if self.layer == "1" else self.getMinDate(engine))
    # Don't think this needs to be here, if it's not present in main it can be removed.

    def set_dates_last_2_quarter_end_l2_all(self, engine):
        # handles vsPrevQuarter date settings.
        if self.layer == "1":
            self.startDate = self.ai.get(
                "startDate", self.get_last_date_of_two_quarters_ago(datetime.utcnow())
            )
            self.endDate = self.ai.get(
                "endDate", self.get_last_date_of_previous_quarter(datetime.utcnow())
            )
        else:
            self.startDate = self.ai.get("startDate", self.getMinDate(engine))
            self.endDate = self.ai.get("endDate", self.getMaxDate(engine))

    def get_last_date_of_previous_quarter(self, dt):
        return (dt - pd.offsets.QuarterEnd()).strftime("%Y-%m-%d")

    def get_last_date_of_two_quarters_ago(self, dt):
        return (dt - pd.offsets.QuarterEnd() - pd.offsets.QuarterEnd()).strftime(
            "%Y-%m-%d"
        )

    # def setDates1Last60Days2TwentyFourFullMonths(self, engine):
    #     self.endDate = self.ai.get('endDate', self.getMaxDate(engine))
    #     self.startDate=self.ai.get('startDate',
    #         (datetime.utcnow() - pd.DateOffset(days=60)).strftime("%Y-%m-%d") if \
    #             self.layer=="1" else x_months_ago(24))

    # def setDates1LastTwoMonths2TwentyFourFullMonths(self, engine):
    #     self.endDate = self.ai.get('endDate', self.getMaxDate(engine))
    #     self.startDate=self.ai.get('startDate',
    #         x_months_ago(2) if self.layer=="1" else x_months_ago(24))

    def set_dates(self, engine, job_metric_dict):
        job_metric_entry = job_metric_dict.get(self.job, {}).get(self.metric, {})
        if "dates_func" in job_metric_entry.keys():
            getattr(self, job_metric_entry["dates_func"])(engine)  # Run the datesFunc.
        else:
            self.set_dates_default(engine)

    def setTargetSQL(self, job_metric_dict):
        self.target = ""
        self.target_cte = ""
        self.target_join = ""
        if self.job != "":
            t_dict = job_metric_dict[self.job][self.metric]
            if "target_class" in t_dict.keys():
                t_obj = t_dict["target_class"](t_dict["target"])
                location_bool, product_bool = t_obj.setTargetSQL()
                if location_bool:
                    self.grouping_inclusions.add("location")
                    self.setGroupingsDict()
                if product_bool:
                    if self.groupings_dict["hierarchy1"]["select"] == "":
                        self.groupings_dict["product_type"][
                            "select"
                        ] = """,
                        product_type"""
                        self.groupings_dict["hierarchy1"][
                            "join"
                        ] = """
                    LEFT JOIN m.pt AS product_type ON TRUE"""
                self.target, self.target_cte, self.target_join = t_obj.getTargetSQL()

    def setFiltersString(self):
        fstr = ""
        for f in self.filters.keys():
            f_insert = f
            if f in valid_hierarchies:
                f_insert = valid_hierarchies[f]
            code = self.filters[f]
            fList = unquote(code).split(",")
            fstr = (
                fstr
                + f"""
            AND {f_insert}::VARCHAR IN ({jnInserter(fList, dropQ=True)})"""
            )
        self.filters_string = fstr

    @abstractmethod
    def getMinDate(self, engine):
        pass

    @abstractmethod
    def getMaxDate(self, engine):
        pass

    @abstractmethod
    def setJobFilter(self):
        pass

    @abstractmethod
    def setGroupingsDict(self):
        pass


class MetricOnlyJobParameters(Params):
    """Subclass of Params for metrics
    that can be pulled from the legacy-CRM."""

    def getMinDate(self, engine):
        return  # Not in use since weather_locations only does L1 / CURRENT_DATE.

    def getMaxDate(self, engine):
        return  # Not in use since weather_locations only does L1 / CURRENT_DATE.

    def setGroupingsDict(self):
        return  # Not in use since weather_locations only does L1.

    def setJobFilter(self, job_metric_dict):
        j_id_dict = {
            "legacy_uk": "jobs.id",
            "nc_us": "jo.job_order_id",
            "1crm_at": "p.id",
            "1crm_de": "p.id",
            "1crm_fr": "p.id",
        }
        j_number_dict = {
            "legacy_uk": "jobs.number",
            "nc_us": "jo.job_number",
            "1crm_at": "j.job_code",
            "1crm_de": "j.job_code",
            "1crm_fr": "j.job_code",
        }
        db = get_db(self.job, job_metric_dict)
        self.job_filter = ""
        if len(self.job) > 0:
            if type(self.job) == int:
                self.job_filter = f"""
            AND {j_id_dict[db]} = {str(self.job)}"""
            else:
                self.job_filter = f"""
            AND {j_number_dict[db]} = '{str(self.job)}'"""


class ParamsBaseLiveLocationDW(Params):
    """
    Parameters(day/week) for live_location metric; drawn from base_live_locations table.
    """

    __slots__ = []

    def getMinDate(self, engine):
        sql = """
            SELECT TO_CHAR(MIN(date), 'YYYY-MM-DD') AS date
            FROM base_live_locations
            WHERE TRUE {job_filter}{filter_wc}
        """
        sql = sql.format(filter_wc=self.filters_string, job_filter=self.job_filter)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
            SELECT TO_CHAR(MIN(date), 'YYYY-MM-DD') AS date
            FROM base_live_locations
            WHERE TRUE {job_filter}{filter_wc}
        """
        sql = sql.format(filter_wc=self.filters_string, job_filter=self.job_filter)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        use_job = getParameterFromJMD("project_id", self)
        if use_job == "":
            use_job = str(self.job)
        self.job_filter = ""
        if len(str(self.job)) > 0:
            self.job_filter = f"""
            AND project_id = '{use_job}'"""

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                                    week"""
            },
            "day_of_week": {
                "select": """,
                                    day_of_week"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class ParamsOnlyProject(Params):
    """Subclass of Params for metrics that are pulled by project_id."""

    def getMinDate(self, engine):
        return  # Not in use since customer_acquisition only does L1 / CURRENT_DATE.

    def getMaxDate(self, engine):
        return  # Not in use since customer_acquisition only does L1 / CURRENT_DATE.

    def setGroupingsDict(self):
        return  # Not in use since customer_acquisition only does L1.

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""
        if len(self.job) > 0:
            self.job_filter = f"""
            AND project_id = {str(self.job)}"""


class ParamsSetTableName(Params):
    """Set table_name: base_lost_x <-- lost; base_pnr_x <-- pnr"""

    __slots__ = ["table_name"]

    def __init__(self, engine, api_input, hash_dict, metric_dict, job_metric_dict):
        self.setTableNames(api_input)
        super(ParamsSetTableName, self).__init__(
            engine, api_input, hash_dict, metric_dict, job_metric_dict
        )

    @abstractmethod
    def setTableNames(self, api_input):
        pass


class ParamsTFFormat(ParamsSetTableName):
    """Subclass of Params for metrics
    that can be pulled directly from TypeForm."""

    __slots__ = []

    @abstractmethod
    def setGroupingsDict(self):
        pass

    def getMinDate(self, engine):
        sql = """
        SELECT TO_CHAR(MIN(date::DATE), 'YYYY-MM-DD') AS date
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        LEFT JOIN m.act AS activity_type ON TRUE
        """
        sql_suffix = """
        WHERE m.field_ref LIKE '{field_ref}%%'{job_filter}{filter_wc}
        ;"""
        if self.field_ref is None:
            sql_suffix = """
        WHERE TRUE{job_filter}{filter_wc}
        ;"""
        sql = sql + sql_suffix
        sql = sql.format(
            field_ref=self.field_ref,
            filter_wc=self.filters_string,
            job_filter=self.job_filter,
            table_name=self.table_name,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        print(df)
        return str(df["date"][0])

    def getMaxDate(self, engine, last_date=None):
        if last_date is None:
            last_date = ""
        else:
            last_date = f"""AND LEFT(date, 10) < {last_date}"""
        sql = """
        SELECT TO_CHAR(MAX(date::DATE), 'YYYY-MM-DD') AS date
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        LEFT JOIN m.act AS activity_type ON TRUE
        """
        sql_suffix = """
        WHERE m.field_ref LIKE '{field_ref}%%'{job_filter}{filter_wc}
          {last_date}
        ;"""
        if self.field_ref is None:
            sql_suffix = """
        WHERE TRUE{job_filter}{filter_wc}
          {last_date}
        ;"""
        sql = sql + sql_suffix
        sql = sql.format(
            field_ref=self.field_ref,
            filter_wc=self.filters_string,
            job_filter=self.job_filter,
            table_name=self.table_name,
            last_date=last_date,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        print(df)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        job_filter = ""
        if len(str(self.job)) > 0:
            if "form_id" in job_metric_dict[self.job][self.metric].keys():
                job_filter = f"""
                  AND m.form_id IN ({jnInserter(
                    job_metric_dict[self.job][self.metric]['form_id'], dropQ=True
                )})"""
        self.job_filter = job_filter


class ParamsTF(ParamsTFFormat):
    __slots__ = []

    def setTableNames(self, api_input):
        self.table_name = "tf_queries"


class MetricTFNoGroupings(ParamsTF):
    """
    Metric with no filters or groupings
    """

    __slots__ = []

    def setGroupingsDict(self):
        self.groupings_dict = {}
        self.filters_dict = {}


class ParamsTFDowWeekTeam(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and
    have only basic (dow, week, old-location) groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "team": {
                "select": """,
                        team""",
            },
        }
        self.handleGroupingsDict(groupings_dict)


class PararmsTFDowWeekLocation(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and have
    only basic (dow, week, old-location) groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "location": {
                "select": """,
                        location""",
            },
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class ParamsTFFormatDowWeekH1H2(ParamsTFFormat):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and have hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "product_type": {
                "select": """,
                    product_type::VARCHAR"""
            },
            "hierarchy1": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
                "join": """
                    LEFT JOIN m.pt AS product_type ON TRUE""",
            },
            "location": {
                "select": """,
                    location""",
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [1, 2])
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDowWeekH1H2(ParamsTFFormatDowWeekH1H2):
    __slots__ = []

    def setTableNames(self, api_input):
        self.table_name = "tf_queries"


class ParamsTFDemoDowWeekH1H2(ParamsTFFormatDowWeekH1H2):
    __slots__ = []

    def setTableNames(self, api_input):
        self.table_name = "tf_demo_queries"


class ParamsTFFormatDowWeekH1H2AgeGender(ParamsTFFormat):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and have hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "product_type": {
                "select": """,
                    product_type::VARCHAR"""
            },
            "hierarchy1": {
                "select": """,
                    CASE {middle_insert}END AS {hx}""",
                "join": """
                LEFT JOIN m.pt AS product_type ON TRUE""",
            },
            "location": {
                "select": """,
                    location""",
            },
            "hierarchy2": {
                "select": """,
                    CASE {middle_insert}END AS {hx}""",
            },
            "age_group": {
                "select": """,
                    age_group"""
            },
            "gender": {
                "select": """,
                    gender"""
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [1, 2])
        # print(groupings_dict)
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDayLocationWeekAgeSex(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and have non-hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "age_group": {
                "select": """,
                    age_group"""
            },
            "sex": {
                "select": """,
                    sex"""
            },
            "location": {
                "select": """,
                    location""",
            },
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
        }
        # print(groupings_dict)
        self.handleGroupingsDict(groupings_dict)

    def setTableNames(self, api_input):
        self.table_name = "tf_queries"


class ParamsTFDayLocationWeekSexFinancialFirm(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and have non-hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "sex": {
                "select": """,
                    sex"""
            },
            "location": {
                "select": """,
                    location""",
            },
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "financial_firm": {
                "select": """, 
                        financial_firm"""
            },
        }
        # print(groupings_dict)
        self.handleGroupingsDict(groupings_dict)

    def setTableNames(self, api_input):
        self.table_name = "tf_queries"


class ParamsTFDowWeekH1H2AgeGender(ParamsTFFormatDowWeekH1H2AgeGender):
    __slots__ = []

    def setTableNames(self, api_input):
        self.table_name = "tf_queries"


class ParamsTFDemoDowWeekH1H2AgeGender(ParamsTFFormatDowWeekH1H2AgeGender):
    __slots__ = []

    def setTableNames(self, api_input):
        self.table_name = "tf_demo_queries"


class ParamsTFDowWeekH2Area(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and have
    activity_type and hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "area": {
                "select": """,
                    area"""
            },
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}"""
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [2])
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDowWeekH1H2Ev(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and
    have event_type and hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "product_type": {
                "select": """,
                        product_type"""
            },
            "hierarchy1": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
                "join": """
                    LEFT JOIN m.pt AS product_type ON TRUE""",
            },
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
            },
            "event_type": {
                "select": """,
                    event_type"""
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [1, 2])
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDowWeekH2Ev(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and
    have event_type and hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
            },
            "event_type": {
                "select": """,
                    event_type"""
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [2])
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDowWeekH2(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and
    have hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [2])
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDowWeekH2H7Team(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and
    have hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}"""
            },
            "project_id": {
                "select": """,
                    project_id"""
            },
            "hierarchy7": {
                "select": """,
                        CASE {middle_insert}END AS {hx}"""
            },
            "team": {
                "select": """,
                        team""",
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [2, 7])
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDowWeekMonthH2(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and
    have Dow, Week, Month, H2 parameters."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "month": {
                "select": """,
                        month"""
            },
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [2])
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDoWWeek(ParamsTF):
    """Inherits from  MetricTFParameters"""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDowWeekMonthYearH1H2Act(ParamsTF):
    """Inherits from  MetricTFParameters for metrics that
    can be pulled directly from TypeForm and
    have dow, week, month, year, activity_type, and hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "week": {
                "select": """,
                        week"""
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "month": {
                "select": """,
                        month"""
            },
            "year": {
                "select": """,
                        year"""
            },
            "product_type": {
                "select": """,
                        product_type::VARCHAR"""
            },
            "hierarchy1": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
                "join": """
                    LEFT JOIN m.pt AS product_type ON TRUE""",
            },
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
            },
            "activity_type": {
                "select": """,
                        activity_type::VARCHAR""",
                "join": """
                    LEFT JOIN m.act AS activity_type ON TRUE""",
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [1, 2])
        # print(groupings_dict)
        self.handleGroupingsDict(groupings_dict)


class ParamsTFDowWeekMonthYearActH1H2H7(ParamsTF):
    """Parameters for global.

    Inherits from MetricTFParameters for metrics that
    can be pulled directly from TypeForm and
    have dow, week, month, year, activity_type, and all hierarchical groupings."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "activity_type": {
                "select": """,
                        activity_type::VARCHAR""",
                "join": """
                    LEFT JOIN m.act AS activity_type ON TRUE""",
            },
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "week": {
                "select": """,
                        week"""
            },
            "month": {
                "select": """,
                        month"""
            },
            "year": {
                "select": """,
                        year"""
            },
            "product_type": {
                "select": """,
                        product_type::VARCHAR"""
            },
            "hierarchy1": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
                "join": """
                    LEFT JOIN m.pt AS product_type ON TRUE""",
            },
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
            },
            "project_id": {
                "select": """,
                    project_id"""
            },
            "hierarchy7": {
                "select": """,
                        CASE {middle_insert}END AS {hx}"""
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [1, 2, 7])
        self.handleGroupingsDict(groupings_dict)


class MetricVideoProfilesParameters(Params):
    __slots__ = []

    def getMinDate(self, engine):
        sql = """
        SELECT  MIN(date)::VARCHAR AS date
        FROM    base_video_dates
        WHERE   TRUE{filter_wc}
        ;"""
        sql = sql.format(filter_wc=self.filters_string)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT  MAX(date)::VARCHAR AS date
        FROM    base_video_dates
        WHERE   TRUE{filter_wc}
        ;"""
        sql = sql.format(filter_wc=self.filters_string)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        self.groupings_dict = {}  # Not yet used.


class MetricActiveParameters(Params):
    __slots__ = []

    def getMinDate(self, engine):
        sql = """
        SELECT  TO_DATE(MIN(date), 'YYYY-MM') AS date
        FROM    base_active_dates
        WHERE   TRUE{filter_wc}
        ;"""
        sql = sql.format(filter_wc=self.filters_string)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT  TO_DATE(MAX(date), 'YYYY-MM') AS date
        FROM    base_active_dates
        WHERE   TRUE{filter_wc}
        ;"""
        sql = sql.format(filter_wc=self.filters_string)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        self.groupings_dict = {}  # Not yet used.


class MetricEnabledStaffParameters(Params):
    """Identical to MetricVideoProfilesParameters save for table. Generalize!"""

    __slots__ = []

    def getMinDate(self, engine):
        sql = """
        SELECT  TO_DATE(MIN(date), 'YYYY-MM') AS date
        FROM    base_staff_dates
        WHERE   TRUE{filter_wc}
        ;"""
        sql = sql.format(filter_wc=self.filters_string)
        # print(sql)
        df = pd.read_sql(sql, engine)
        print("MetricEnabledStaffParameters().getMinDate()'s df:")
        print(df)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT  TO_DATE(MAX(date), 'YYYY-MM') AS date
        FROM    base_staff_dates
        WHERE   TRUE{filter_wc}
        ;"""
        sql = sql.format(filter_wc=self.filters_string)
        # print(sql)
        df = pd.read_sql(sql, engine)
        print("MetricEnabledStaffParameters().getMaxDate()'s df:")
        print(df)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        self.groupings_dict = {}  # Not yet used.


class MetricRegionProjectManagerParameters(ParamsSetTableName):
    """Set table_name: base_lost_x <-- lost; base_pnr_x <-- pnr"""

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM (  SELECT date{h3}{h4}{h5}
                FROM base_{table_name}_dates
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(
            h3=self.gfie("hierarchy3"),
            h4=self.gfie("hierarchy4"),
            h5=self.gfie("hierarchy5"),
            filter_wc=self.filters_string,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT MAX(date) AS date
        FROM (  SELECT date{h3}{h4}{h5}
                FROM base_{table_name}_dates
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(
            h3=self.gfie("hierarchy3"),
            h4=self.gfie("hierarchy4"),
            h5=self.gfie("hierarchy5"),
            filter_wc=self.filters_string,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        groupings_dict = {
            "hierarchy3": {
                "select": """,
                        region AS {vh_at_hx}, 
                        CASE {middle_insert}END AS {hx}""",
                "join": """""",
            },
            "hierarchy4": {
                "select": """,
                        project AS {vh_at_hx}, 
                        CASE {middle_insert}END AS {hx}""",
                "join": """""",
            },
            "hierarchy5": {
                "select": """,
                        manager AS {vh_at_hx}, 
                        CASE {middle_insert}END AS {hx}""",
                "join": """""",
            },
        }
        for i in [3, 4, 5]:
            hx = f"hierarchy{i}"
            if hx in self.filters:
                sql_insert = groupings_dict[hx]["select"]
                middle_insert = ""
                for key, value in self.hfd[f"{hx}_dict"].items():  # grouping_node_dict
                    repkey = key.replace("'", "\\'")
                    h_insert = f"""WHEN {valid_hierarchies[hx]} IN ({jnInserter(
                        value, dropQ=True)}) THEN '{repkey}'
                                         """
                    middle_insert += h_insert
                sql_insert = sql_insert.format(
                    middle_insert=middle_insert, hx=hx, vh_at_hx=valid_hierarchies[hx]
                )
                groupings_dict[hx]["select"] = sql_insert
        # print(groupings_dict)
        self.handleGroupingsDict(groupings_dict)


class MetricPNRRegionProjectManagerParameters(MetricRegionProjectManagerParameters):
    """Set table_name: pnr --> base_pnr_x"""

    def setTableNames(self, api_input):
        self.table_name = "pnr"


class MetricLostRegionProjectManagerParameters(MetricRegionProjectManagerParameters):
    """Set table_name: lost --> base_lost_x"""

    def setTableNames(self, api_input):
        self.table_name = "lost"


class MetricClientNPSParameters(Params):
    __slots__ = []

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM (  SELECT date
                FROM base_client_nps_dates
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(
            region=self.gfie("region"),
            h6=self.gfie("hierarchy6"),
            filter_wc=self.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT MAX(date) AS date
        FROM (  SELECT date
                FROM base_client_nps_dates
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(
            region=self.gfie("region"),
            h6=self.gfie("hierarchy6"),
            filter_wc=self.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        groupings_dict = {
            "region": {
                "select": """,
                        region"""
            },
            "hierarchy6": {
                "select": """,
                        hierarchy6""",
                "join": """
                LEFT JOIN ( SELECT  response_id, 
                                    client AS {vh_at_hx}, 
                                    CASE {middle_insert}END AS {hx}
                            FROM base_client_nps
                ) h6 ON h6.response_id = m.response_id""",
            },
        }
        hx = "hierarchy6"
        if hx in self.filters:
            sql_insert = groupings_dict[hx]["join"]
            middle_insert = ""
            for key, value in self.hfd[f"{hx}_dict"].items():  # grouping_node_dict
                repkey = key.replace("'", "\\'")
                h_insert = f"""WHEN {valid_hierarchies[hx]} IN ({jnInserter(
                    value, dropQ=True)}) THEN '{repkey}'
                                        """
                middle_insert += h_insert
            sql_insert = sql_insert.format(
                middle_insert=middle_insert, hx=hx, vh_at_hx=valid_hierarchies[hx]
            )
            groupings_dict[hx]["join"] = sql_insert
        # print(groupings_dict)
        self.handleGroupingsDict(groupings_dict)


class MetricBaseRegionParameters(ParamsSetTableName):
    """Set table_name: base_lost_x <-- lost; base_pnr_x <-- pnr"""

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM (  SELECT date
                FROM base_{table_name}_dates
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(filter_wc=self.filters_string, table_name=self.table_name)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT MAX(date) AS date
        FROM (  SELECT date
                FROM base_{table_name}_dates
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(filter_wc=self.filters_string, table_name=self.table_name)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        groupings_dict = {
            "region": {
                "select": """,
                        region"""
            }
        }
        self.handleGroupingsDict(groupings_dict)


class MetricShiftsOpenRegionParameters(MetricBaseRegionParameters):
    """Set table_name: lost --> base_lost_x"""

    def setTableNames(self, api_input):
        self.table_name = "shifts_open"


class ParamsCrmOcRegion(MetricBaseRegionParameters):
    """Set table_name: lost --> base_lost_x"""

    def setTableNames(self, api_input):
        self.table_name = "onboarding_conversion"


class MetricOnlyDateParameters(Params):
    """Subclass of Params for metrics that
    can be pulled from the legacy-CRM."""

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM base_{metric}_dates
        ;"""
        sql = sql.format(metric=self.metric)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT MAX(date) AS date
        FROM base_{metric}_dates
        ;"""
        sql = sql.format(metric=self.metric)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        self.groupings_dict = {}  # Not yet used.


class MetricFreelancerUserDateParameters(Params):
    """Subclass of Params for metrics 1CRM freelance and user stuff;
    for zendesk_exports."""

    def getMinDate(self, engine):
        start_sql = """
        WITH tbl AS (
        """
        recency_query = """
            SELECT updated_at
            FROM users{suffix}
            UNION
            SELECT updated_at
            FROM freelancers{suffix}
        """
        end_sql = """
        ) 
        SELECT MIN(updated_at)::DATE AS date FROM tbl
        ;"""
        sql = makeMultiDBQuery(recency_query, start_sql, "            UNION", end_sql)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        start_sql = """
        WITH tbl AS (
        """
        recency_query = """
            SELECT updated_at
            FROM users{suffix}
            UNION
            SELECT updated_at
            FROM freelancers{suffix}
        """
        end_sql = """
        ) 
        SELECT MAX(updated_at)::DATE AS date FROM tbl
        ;"""
        sql = makeMultiDBQuery(recency_query, start_sql, "            UNION", end_sql)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        self.groupings_dict = {}  # Not yet used.


class MetricComplianceDashParameters(Params):
    """Set table_name: base_correction_rate <-- correction_rate;"""

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM (  SELECT  date{dow}{week}{month}{year}
                FROM base_{metric}_dates
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(
            dow=self.gfie("day_of_week"),
            week=self.gfie("week"),
            month=self.gfie("month"),
            year=self.gfie("year"),
            filter_wc=self.filters_string,
            metric=self.metric,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT MAX(date) AS date
        FROM (  SELECT  date{dow}{week}{month}{year}
                FROM base_{metric}_dates
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(
            dow=self.gfie("day_of_week"),
            week=self.gfie("week"),
            month=self.gfie("month"),
            year=self.gfie("year"),
            filter_wc=self.filters_string,
            metric=self.metric,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        groupings_dict = {
            "activity_type": {
                "select": """,
                        activity_type"""
            },
            "hierarchy1": {
                "select": """,
                        hierarchy1""",
                "join": """
                LEFT JOIN (SELECT   response_id,
                                    product_type,
                                    CASE {middle_insert}END AS {hx}
                        FROM base_{metric}) h1 ON h1.response_id = m.response_id""",
            },
            "hierarchy2": {
                "select": """,
                        hierarchy2""",
                "join": """
                LEFT JOIN (SELECT   response_id,
                                    location,
                                    CASE {middle_insert}END AS {hx}
                           FROM base_{metric}) h2 ON h2.response_id = m.response_id""",
            },
            "year": {
                "select": """,
                        LEFT(date::VARCHAR, 4) AS year"""
            },
            "month": {
                "select": """,
                        LEFT(date::VARCHAR, 7) AS month"""
            },
            "week": {
                "select": """,
                        TO_CHAR(DATE_TRUNC('week', date), 'YYYY-MM-DD') AS week""",
            },
            "day_of_week": {
                "select": """,
                        TRIM(' ' FROM TO_CHAR(date, 'Day')) AS day_of_week""",
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [1, 2])
        # print(groupings_dict)
        self.handleGroupingsDict(groupings_dict)


class MetricComplianceDashDateNoParameters(Params):
    """Subclass of Params for compliance-dash metrics
    where we filter dates, but don't group."""

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM base_{metric}_dates;"""
        sql = sql.format(metric=self.metric)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT MAX(date) AS date
        FROM base_{metric}_dates"""
        sql = sql.format(metric=self.metric)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        groupings_dict = {}
        self.handleGroupingsDict(groupings_dict)


class MetricDateBasedParameters(ParamsSetTableName):
    __slots__ = []

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM (  SELECT *{dow}{week}{month}{year}
                FROM {table_name}
                WHERE TRUE{filter_wc});"""
        sql = sql.format(
            dow=self.gfie("day_of_week"),
            week=self.gfie("week"),
            month=self.gfie("month"),
            year=self.gfie("year"),
            filter_wc=self.filters_string,
            table_name=self.table_name,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        print(df)
        return str(df["date"][0])

    def getMaxDate(self, engine):
        sql = """
        SELECT MAX(date) AS date
        FROM (  SELECT *{dow}{week}{month}{year}
                FROM {table_name}
                WHERE TRUE{filter_wc});"""
        sql = sql.format(
            dow=self.gfie("day_of_week"),
            week=self.gfie("week"),
            month=self.gfie("month"),
            year=self.gfie("year"),
            filter_wc=self.filters_string,
            table_name=self.table_name,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        print(df)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def setGroupingsDict(self):
        groupings_dict = {
            "year": {
                "select": """,
                        LEFT(date::VARCHAR, 4) AS year"""
            },
            "month": {
                "select": """,
                        LEFT(date::VARCHAR, 7) AS month"""
            },
            "week": {
                "select": """,
                        TO_CHAR(DATE_TRUNC('week', date), 'YYYY-MM-DD') AS week"""
            },
            "day_of_week": {
                "select": """,
                        TRIM(' ' FROM TO_CHAR(date, 'Day')) AS day_of_week"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class MetricComplianceDashDateParameters(MetricDateBasedParameters):
    """Subclass of Params for compliance-dash metrics where
    we filter dates & group by day of week, week."""

    def setTableNames(self, api_input):
        self.table_name = f"base_{api_input.get('metric')}_dates"


class MetricBTTParameters(Params):
    def setGroupingsDict(self):
        groupings_dict = {
            "month": {
                "select": """,
                    month"""
            },
            "region": {
                "select": """,
                    region"""
            },
        }
        self.handleGroupingsDict(groupings_dict)

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM sus_business_travel_tracker
        WHERE TRUE{filter_wc}
        """
        print("getMinDate's sql")
        print(sql)
        sql = sql.format(filter_wc=self.filters_string)
        print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine, last_date=None):
        if last_date is None:
            last_date = ""
        else:
            last_date = f"""AND LEFT(date, 10) < {last_date}"""
        sql = """
        SELECT MAX(date) AS date
        FROM sus_business_travel_tracker
        WHERE TRUE{filter_wc}
        """
        print("getMaxDate's sql")
        print(sql)
        sql = sql.format(filter_wc=self.filters_string)
        print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])


class MetricUberParameters(Params):
    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM (  SELECT *{dow}{week}
                FROM uber_applies_redemption
                WHERE TRUE{job_filter}{filter_wc});"""
        sql = sql.format(
            dow=self.gfie("day_of_week"),
            week=self.gfie("week"),
            job_filter=self.job_filter,
            filter_wc=self.filters_string,
            metric=self.metric,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        print(df)
        return str(df["date"][0])

    def getMaxDate(self, engine, last_date=None):
        if last_date is None:
            last_date = ""
        else:
            last_date = f""" AND date < {last_date}"""
        sql = """
        SELECT MAX(date) AS date
        FROM (  SELECT *{dow}{week}
                FROM uber_applies_redemption
                WHERE TRUE{job_filter}{filter_wc}{last_date});"""
        sql = sql.format(
            dow=self.gfie("day_of_week"),
            week=self.gfie("week"),
            job_filter=self.job_filter,
            filter_wc=self.filters_string,
            metric=self.metric,
            last_date=last_date,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        print(df)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        job_filter = ""
        if len(self.job) > 0:
            job_filter = f""" 
              AND project_id = '{self.job}'"""
        self.job_filter = job_filter

    def setGroupingsDict(self):
        groupings_dict = {
            "location": {
                "select": """,
                    location"""
            },
            "hierarchy2": {
                "select": """,
                    CASE {middle_insert}END AS {hx}""",
            },
            "week": {
                "select": """,
                        TO_CHAR(DATE_TRUNC('week', date), 'YYYY-MM-DD') AS week"""
            },
            "day_of_week": {
                "select": """,
                        TRIM(' ' FROM TO_CHAR(date, 'Day')) AS day_of_week"""
            },
        }
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [2])
        self.handleGroupingsDict(groupings_dict)


class SimpleProjectLessParameters(ParamsSetTableName):
    """For metrics without project filters. Set ParamsSetTableName."""

    @abstractmethod
    def setTableNames(self, api_input):
        pass

    def getMinDate(self, engine):
        sql = """
        SELECT MIN(date) AS date
        FROM (  SELECT  date
                FROM {table_name}
                WHERE TRUE{filter_wc})
        ;"""
        sql = sql.format(filter_wc=self.filters_string, table_name=self.table_name)
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine, last_date=None):
        if last_date is None:
            last_date = ""
        else:
            last_date = f""" AND date < {last_date}"""
        sql = """
        SELECT MAX(date) AS date
        FROM (  SELECT  date
                FROM {table_name}
                WHERE TRUE{filter_wc}{last_date})
        ;"""
        sql = sql.format(
            filter_wc=self.filters_string,
            last_date=last_date,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        self.job_filter = ""  # Not yet used.


class SimpleYearRegionParameters(SimpleProjectLessParameters):
    """For metrics from Salesforce. Set SimpleProjectLessParameters."""

    @abstractmethod
    def setTableNames(self, api_input):
        pass

    def setGroupingsDict(self):
        groupings_dict = {
            "year": {
                "select": """,
                        year"""
            },
            "region": {
                "select": """,
                        region"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class SimpleYearMonthRegionParameters(SimpleProjectLessParameters):
    """For metrics from Salesforce. Set SimpleProjectLessParameters."""

    @abstractmethod
    def setTableNames(self, api_input):
        pass

    def setGroupingsDict(self):
        groupings_dict = {
            "year": {
                "select": """,
                        year"""
            },
            "month": {
                "select": """,
                        month"""
            },
            "region": {
                "select": """,
                        region"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class SimpleDayofWeekWeekParameters(SimpleProjectLessParameters):
    """For metrics from Salesforce. Set SimpleProjectLessParameters."""

    @abstractmethod
    def setTableNames(self, api_input):
        pass

    def setGroupingsDict(self):
        groupings_dict = {
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "week": {
                "select": """,
                        week"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class ParametersSimpleDayofWeekWeekLocation(SimpleProjectLessParameters):
    """For metrics from Salesforce. Set SimpleProjectLessParameters."""

    @abstractmethod
    def setTableNames(self, api_input):
        pass

    def setGroupingsDict(self):
        groupings_dict = {
            "day_of_week": {
                "select": """,
                        day_of_week"""
            },
            "week": {
                "select": """,
                        week"""
            },
            "location": {
                "select": """,
                        location"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class ParametersRiaDayofWeekWeekLocation(ParametersSimpleDayofWeekWeekLocation):
    def setTableNames(self, api_input):
        self.table_name = "coupon_redemption_ria01001"


class ParamsXfactrTargetActual(SimpleDayofWeekWeekParameters):
    def setTableNames(self, api_input):
        self.table_name = "stores_activated_xfctr"


class MetricSFStatusParameters(SimpleYearMonthRegionParameters):
    """For metrics from Salesforce. Set ParamsSetTableName."""

    def setTableNames(self, api_input):
        self.table_name = "sf_impact_project_status_report"


class MetricSFRevenueParameters(SimpleYearMonthRegionParameters):
    """For metrics from Salesforce. Set ParamsSetTableName."""

    def setTableNames(self, api_input):
        self.table_name = "sf_impact_api_integration_report_2"


class MetricEmpMOTParameters(SimpleYearRegionParameters):
    def setTableNames(self, api_input):
        self.table_name = "sus_employee_mot"


class MetricEmployeeDistanceParameters(SimpleYearRegionParameters):
    def setTableNames(self, api_input):
        self.table_name = "sus_employee_travel_distances"


class MetricBusMOTParameters(SimpleYearRegionParameters):
    def setTableNames(self, api_input):
        self.table_name = "sus_business_mot"


class MetricBusDistanceParameters(SimpleYearRegionParameters):
    def setTableNames(self, api_input):
        self.table_name = "sus_business_distances"


class MetricEnergyUseParameters(SimpleProjectLessParameters):
    """See SimpleProjectLessParameters."""

    def setTableNames(self, api_input):
        self.table_name = "sus_gas_electric_tracker"

    def setGroupingsDict(self):
        groupings_dict = {
            "year": {
                "select": """,
                        year"""
            },
            "region": {
                "select": """,
                        region"""
            },
            "fuel": {
                "select": """,
                    fuel"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class MetricSusComplianceParameters(SimpleYearMonthRegionParameters):
    # Previously handled: year, month, week
    def setTableNames(self, api_input):
        self.table_name = "sus_field_staff_compliance_dates"


class MetricSusFieldStaffParameters(SimpleYearRegionParameters):
    # Previously handled: year, month, week, day_of_week
    def setTableNames(self, api_input):
        self.table_name = "sus_field_staff_sustainability_dates"


class MetricBusCFScoreParams(SimpleYearRegionParameters):
    def setTableNames(self, api_input):
        self.table_name = "sus_business_cfs"


class MetricCombinedCFScoreParams(SimpleYearRegionParameters):
    def setTableNames(self, api_input):
        self.table_name = "sus_total_cfs"


class MetricTreesPlantedParameters(SimpleProjectLessParameters):
    """For metrics from Salesforce. Set SimpleProjectLessParameters."""

    def setTableNames(self, api_input):
        self.table_name = "sus_trees_planted"

    def setGroupingsDict(self):
        groupings_dict = {
            "year": {
                "select": """
                    year"""
            },
            "client": {
                "select": """,
                    client"""
            },
        }
        self.handleGroupingsDict(groupings_dict)


class MetricPostTrackingFormatParameters(ParamsSetTableName):
    """Subclass of Params for metrics that rely on post-tracking data."""

    __slots__ = []

    def setGroupingsDict(self):
        groupings_dict = {
            "product_type": {
                "select": """,
                    product_type::VARCHAR"""
            },
            "hierarchy1": {
                "select": """,
                        CASE {middle_insert}END AS {hx}""",
                "join": """
                    LEFT JOIN m.pt AS product_type ON TRUE""",
            },
        }
        for i in [
            "age_group",
            "gender",
            "persona",  # "day_of_week", "week", "year",
            "months_after",
        ]:
            groupings_dict = addSimpleGroupingsDictEntry(i, groupings_dict)
        groupings_dict = self.handleHierarchicalGroupingsDict(groupings_dict, [1, 2])
        # print(groupings_dict)
        self.handleGroupingsDict(groupings_dict)

    def getMinDate(self, engine):
        sql = """
        SELECT TO_CHAR(MIN(date::DATE), 'YYYY-MM-DD') AS date
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        """
        sql_suffix = """
        WHERE m.field_ref LIKE '{field_ref}%%'{job_filter}{filter_wc}
        ;"""
        if self.field_ref is None:
            sql_suffix = """
        WHERE TRUE{job_filter}{filter_wc}
        ;"""
        sql = sql + sql_suffix
        sql = sql.format(
            field_ref=self.field_ref,
            filter_wc=self.filters_string,
            job_filter=self.job_filter,
            table_name=self.table_name,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine, last_date=None):
        if last_date is None:
            last_date = ""
        else:
            last_date = f"""AND LEFT(date, 10) < {last_date}"""
        sql = """
        SELECT TO_CHAR(MAX(date::DATE), 'YYYY-MM-DD') AS date
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        """
        sql_suffix = """
        WHERE m.field_ref LIKE '{field_ref}%%'{job_filter}{filter_wc}
          {last_date}
        ;"""
        if self.field_ref is None:
            sql_suffix = """
        WHERE TRUE{job_filter}{filter_wc}
          {last_date}
        ;"""
        sql = sql + sql_suffix
        sql = sql.format(
            field_ref=self.field_ref,
            filter_wc=self.filters_string,
            job_filter=self.job_filter,
            table_name=self.table_name,
            last_date=last_date,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        # print(df)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        job_filter = ""
        if len(self.job) > 0:
            if "project_id" in job_metric_dict[self.job][self.metric].keys():
                job_filter = f"""
                  AND m.project_id IN ({jnInserter(
                    job_metric_dict[self.job][self.metric]['project_id'], dropQ=True
                )})"""
        self.job_filter = job_filter


class MetricPostTrackingParameters(MetricPostTrackingFormatParameters):
    """Subclass of MetricPostTrackingFormatParameters; for actual metrics."""

    __slots__ = []

    def setTableNames(self, api_input):
        self.table_name = "tf_post_tracking"


class MetricPostTrackingDemoParameters(MetricPostTrackingFormatParameters):
    """Subclass of MetricPostTrackingFormatParameters;
    like MetricPostTrackingParameters but for demo metrics."""

    __slots__ = []

    def setTableNames(self, api_input):
        self.table_name = "tf_demo_post_tracking"


class MetricInKaiduParameters(Params):
    def setGroupingsDict(self):
        pass  # Not needed?

    def getMinDate(self, engine):
        sql = """
        SELECT TO_CHAR(MIN(date), 'YYYY-MM-DD') AS date
        FROM kaidu
        WHERE value != 0{job_filter}{filter_wc}
        ;"""
        sql = sql.format(filter_wc=self.filters_string, job_filter=self.job_filter)
        print(sql)
        df = pd.read_sql(sql, engine)
        print(df)
        return str(df["date"][0])

    def getMaxDate(self, engine, last_date=None):
        if last_date is None:
            last_date = ""
        else:
            last_date = f"""AND LEFT(date, 10) < {last_date}"""
        sql = """
        SELECT TO_CHAR(MAX(date), 'YYYY-MM-DD') AS date
        FROM kaidu
        WHERE value != 0{job_filter}{filter_wc}
          {last_date}
        ;"""
        sql = sql.format(
            filter_wc=self.filters_string,
            job_filter=self.job_filter,
            last_date=last_date,
        )
        print(sql)
        df = pd.read_sql(sql, engine)
        print(df)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        use_cid = getParameterFromJMD("customer_id", self)
        self.job_filter = ""
        if len(self.job) > 0:
            self.job_filter = f"""
            AND customer_id = '{use_cid}'"""


# class MetricInKaiduDowHZParameters(MetricInKaiduParameters):

#     def setGroupingsDict(self):
#         groupings_dict = {
#             "day_of_week":{"select":""",
#                 day_of_week"""},
#             "hour":{"select":""",
#                 hour"""},
#             "zone":{"select":""",
#                 zone"""}
#         }
#         self.handleGroupingsDict(groupings_dict)
# class MetricInKaiduDowZParameters(MetricInKaiduParameters):

#     def setGroupingsDict(self):
#         groupings_dict = {
#             "day_of_week":{"select":""",
#                 day_of_week"""},
#             "hour":{"select":""},
#             "zone":{"select":""",
#                 zone"""}
#         }
#         self.handleGroupingsDict(groupings_dict)
# class MetricInKaiduDowHParameters(MetricInKaiduParameters):

#     def setGroupingsDict(self):
#         groupings_dict = {
#             "day_of_week":{"select":""",
#                 day_of_week"""},
#             "hour":{"select":""",
#                 hour"""},
#             "zone":{"select":""}
#         }
#         self.handleGroupingsDict(groupings_dict)


class MetricInSimpleParameters(ParamsSetTableName):
    """Subclass of Params
    for metrics that can be pulled directly from TypeForm."""

    __slots__ = []

    def setGroupingsDict(self):
        pass  # Not needed.

    def getMinDate(self, engine):
        sql = """
        SELECT TO_CHAR(MIN(date)::DATE, 'YYYY-MM-DD') AS date
        FROM {table_name} m
        WHERE TRUE{job_filter}{filter_wc}
        ;"""
        sql = sql
        sql = sql.format(
            filter_wc=self.filters_string,
            job_filter=self.job_filter,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        return str(df["date"][0])

    def getMaxDate(self, engine, last_date=None):
        if last_date is None:
            last_date = ""
        else:
            last_date = f"""AND LEFT(date, 10) < {last_date}"""
        sql = """
        SELECT TO_CHAR(MAX(date)::DATE, 'YYYY-MM-DD') AS date
        FROM {table_name} m
        WHERE TRUE{job_filter}{filter_wc}
          {last_date}
        ;"""
        sql = sql
        sql = sql.format(
            filter_wc=self.filters_string,
            job_filter=self.job_filter,
            table_name=self.table_name,
            last_date=last_date,
        )
        # print(sql)
        df = pd.read_sql(sql, engine)
        # print(df)
        return str(df["date"][0])

    def setJobFilter(self, job_metric_dict):
        job_filter = ""
        if len(self.job) > 0:
            if "project_id" in job_metric_dict[self.job][self.metric].keys():
                job_filter = f"""
                  AND m.project_id IN ({jnInserter(
                    job_metric_dict[self.job][self.metric]['project_id'], dropQ=True
                )})"""
        self.job_filter = job_filter


class MetricInSquareParameters(MetricInSimpleParameters):
    __slots__ = []

    def setTableNames(self, api_input):
        self.table_name = "square_o365"


########################################
### /end Parameter Gathering Classes ###
########################################

########################
### Helper Functions ###
########################


def addSimpleGroupingsDictEntry(grouping, groupings_dict):
    groupings_dict[grouping] = {
        "select": f""",
            {grouping}"""
    }
    return groupings_dict


#############################
### /end Helper Functions ###
#############################
