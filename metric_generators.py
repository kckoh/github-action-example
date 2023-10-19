"""Classes to actually calculate the metrics."""
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import json
import pandas as pd
from uuid import uuid1
import boto3
from botocore.exceptions import ClientError
import logging
from numbers import Number
import re
from typing import Optional

from helpers import (
    jnInserter,
    getParameterFromJMD,
    makeMultiDBQuery,
    get_db,
    human_format,
    valid_hierarchies,
    x_months_ago,
    format_string_with_dict,
)  # findNth,

from impactlib import get_config

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 10000)
config = get_config()
bucket = config["s3"].get("BUCKET_STRING")
aws_creds = {
    "region_name": "eu-west-2",
    "aws_access_key_id": config["aws"].get("aws_access_key_id"),
    "aws_secret_access_key": config["aws"].get("aws_secret_access_key"),
}

#################################
### Metric Generating Classes ###
#################################


class MetricFromParameters(ABC):
    """Object generating and running DB query."""

    __slots__ = [
        "engine",
        "params",
        "ff",
        "useMethod",
        "valid_groupings",
        "valid_filters",
        "startDate",
        "endDate",
        "startAt",
        "endAt",
        "dateArray",
        "filters",
        "return_dict",
        "df",
        "return_json",
        "metric_dict",
        "job_metric_dict",
        "ui",
        "l1",
        "l2_overlay",
        "target",
        "target_insert",
    ]

    def __init__(self, engine, ParameterClass, metric_dict, job_metric_dict={}):
        """Initialize class."""
        self.engine = engine
        self.params = ParameterClass
        self.startDate = self.params.startDate
        self.endDate = self.params.endDate
        self.metric_dict = metric_dict
        self.job_metric_dict = job_metric_dict
        self.ui = uuid1().__str__().replace("-", "")  # unique_identifier
        FilterClass = metric_dict[self.params.metric]["filter_class"]
        self.target = ""
        self.target_insert = ""
        job = self.params.job
        metric = self.params.metric
        if job in self.job_metric_dict.keys():
            if metric in self.job_metric_dict[job].keys():
                if "filter_class" in self.job_metric_dict[job][metric].keys():
                    FilterClass = self.job_metric_dict[job][metric]["filter_class"]
                if "target" in self.job_metric_dict[job][metric].keys():
                    self.target = self.job_metric_dict[job][metric]["target"]
                    self.target_insert = ", target"
        self.ff = FilterClass(self.engine, self)
        self.pickMethod()

    def pickMethod(self):
        method_dict = {
            "1": self.getL1,
            "2": self.getL2,
            "filters": self.ff.getValidFilters,
            "groupings": self.getGroupings,
            "csv": self.handleFullExport,
            "name": self.getProjectName,
        }
        self.useMethod = method_dict[self.params.layer]

    def handleFullExport(self):
        self.setFullExport()

    def getGroupings(self):
        self.ff.setValidGroupings()
        self.setAllDates()
        # Where do we get a list of valid groupings?
        # From the metric by default; from the job-metric if available;
        # see filters.py's setValidGroupings() method;
        # FilterClass specified in metric_dict / job_metric_dict.
        self.filters = {}
        for hx in valid_hierarchies:
            if hx in self.valid_groupings.keys():
                self.ff.setGenericOptions(hx)
                df = self.getTopHXAncestry(self.filters[hx], hx)
                zero_list = sorted(list(df["node_id"]))
                hx_default_str = ",".join([str(x) for x in zero_list])
                self.valid_groupings[hx]["defaults"] = hx_default_str
        self.return_dict = {
            "date": {
                "startDate": self.startDate,
                "endDate": self.endDate,
                "startAt": self.startAt,
                "endAt": self.endAt,
                "dateArray": self.dateArray,
            },
            "groupings": self.valid_groupings,
        }

    def getTopHXAncestry(self, node_names, hierarchyx):
        """node_names is a list of node names
        x is the hierarchy_id."""
        nns = jnInserter(node_names, dropQ=True, dropNull=True)
        # print(nns)
        sql = f"""
        SELECT  DISTINCT last AS node_id
        FROM (  SELECT  REVERSE(SPLIT_PART(REVERSE(JSON_SERIALIZE(lineage)), ',', 1)) 
                            AS last_chunk, 
                        TRIM('[]' FROM last_chunk) AS last
                FROM hierarchical_groupings
                WHERE hierarchy_id = {hierarchyx[-1]}
                AND node_name IN ({nns})
                ORDER BY node_id) 
        ;"""
        return self.make_df(sql, to_self=False)

    def make_months_list(self, include_start=False):
        dl = (
            pd.date_range(self.params.startDate, self.params.endDate, freq="MS")
            .strftime("%Y-%m")
            .tolist()
        )
        if include_start:
            dl.append(self.params.startDate[:7])
        return jnInserter(dl)

    def make_quarters_list(self, include_start=False):
        dl = (
            pd.period_range(self.params.startDate, self.params.endDate, freq="Q")
            .strftime("%Y-Q%q")
            .tolist()
        )
        if include_start:
            dl.append(self.params.startDate[:7])
        print(dl)
        return jnInserter(dl)

    def make_df(self, sql, to_self=True):
        if self.params.debug:
            print(sql)
        df = pd.read_sql(sql, self.engine)
        if to_self:
            self.df = df
        if self.params.debug:
            print(df)
        return df

    # @abstractmethod
    # def setOptions(self):
    #     pass

    def getProjectName(self):
        job = self.params.job
        db = get_db(job, self.job_metric_dict)
        pick_project_name_dict = {
            "legacy_uk": f"""
        SELECT name 
        FROM jobs
        WHERE number = '{self.params.job}'
        ;""",
            "nc_us": f"""
        SELECT LISTAGG(jo.title, ', ') WITHIN GROUP (ORDER BY jo.title) AS name
        FROM job_orders_nc jo 
        WHERE job_number = '{self.params.job}'
        ;""",
            "1crm_at": "",
            "1crm_de": "",
            "1crm_fr": "",
        }
        df = self.make_df(pick_project_name_dict[db])
        self.return_dict = df.to_dict(orient="records")
        return self.df

    @abstractmethod
    def setAllDates(self):
        pass

    def standardDatesProcessing(self, dts):
        dts = list(dts["date"])
        dts = list(filter(None, dts))
        if "" in dts:
            dts.remove("")
        dr = pd.date_range(dts[0], dts[-1])
        da = dr.isin(dts)  # date array
        da = [int(i) for i in da]
        self.startDate = str(dts[0])
        self.endDate = str(dts[-1])
        self.startAt = self.startDate
        self.endAt = self.endDate
        self.dateArray = da

    def setAllDatesSimple(self, table_name, include_job_filter=True):
        jf = ""
        if include_job_filter:
            jf = self.params.job_filter
        sql = f"""
        SELECT DISTINCT date
        FROM {table_name} m
        WHERE date IS NOT NULL{jf}
        ORDER BY date
        ;"""
        dts = pd.read_sql(sql, self.engine)
        self.standardDatesProcessing(dts)

    @abstractmethod
    def setL1(self):
        pass

    def getL1(self):
        # if self.params.startDate is None or self.params.startDate == 'None':
        #     self.return_dict = pd.DataFrame({
        #         self.params.metric:["No valid dates."]}).to_dict(orient='records')
        #     return
        self.setL1()
        self.l1.get_l1()
        if len(self.df.index) > 0:
            metric = self.params.metric
            self.df[metric] = self.df[metric].astype(str)
            if len(self.df[metric][0]) > 13:
                self.df[metric][0] = self.df[metric][0][:10] + "..."
        # else:
        #     self.df = pd.DataFrame({self.params.metric:["No data."]})
        self.return_dict = self.df.to_dict(orient="records")

    @abstractmethod
    def setL2(self):
        pass

    def set_return_dict(self):
        self.return_dict = self.df.to_dict(orient="records")

    def getL2(self):
        self.setL2()
        self.df = self.df.fillna("Blank")
        for i in ["week"]:
            if i in self.df.columns:
                self.df[i] = self.df[i].astype(str)
        if "target" in self.df.columns:
            self.df["target"] = self.df["target"].apply(round, ndigits=0)
        self.set_return_dict()

    @abstractmethod
    def setFullExport(self):
        pass

    def useS3(self, suffix="json", use_json=False):
        date_now = datetime.now()
        file_key = f"impact_2_api/temp_{date_now}_v3_\
            {self.params.metric}_export.{suffix}"
        jsbucket = S3JsonBucket(bucket)  # print("Getting s3bucket.")
        jsbucket.dump(file_key, self.return_dict, use_json=use_json)  # "Pushing to s3."
        psurl = createPresignedUrl(file_key)  # print("Getting ps url.")
        print("psurl")
        print(psurl)
        self.return_dict = json.dumps({"s3_link": psurl})
        # print(self.return_json)

    def setReturnJson(self):
        self.return_json = json.dumps(
            {
                "date": {
                    "UTC": (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + " UTC")
                },
                "startDate": self.startDate,
                "endDate": self.endDate,
                "table": self.return_dict,
            }
        )

    def getGroup(self, param, frm="select"):
        """Gets self.params.groupings_dict[param][frm] if it exists,
        otherwise returns empty string."""
        return self.params.groupings_dict.get(param, {}).get(frm, "")

    def create_date_index(
        self,
        date_format="YYYY-MM-DD",
        date_increment="(startDate::DATE + counter)",
        reverse_increm="endDate::DATE - counter",
        set_start_to_01=False,
    ):
        # Creates index of 'YYYY-MM-DD'-formatted date-strings.
        """ui: unique identifier, string to make 'date_index_{ui}' a unique table
            and create_index{ui} a unique procedure.
        startDate: yyyy-mm-dd-formatted string of starting-date.
        endDate: yyyy-mm-dd-formatted string of ending-date."""
        start_date = self.params.startDate
        if set_start_to_01:
            start_date = f"{start_date[:7]}-01"
        end_date = self.params.endDate
        ui = self.ui
        return f"""
        CREATE OR REPLACE PROCEDURE create_index{ui}(
            startDate INOUT VARCHAR, endDate INOUT VARCHAR
        ) AS $$
        DECLARE
            counter INT := 0;
        BEGIN
            WHILE {date_increment} <= endDate::DATE LOOP
                INSERT INTO date_index_{ui}(date_str)
                VALUES(TO_CHAR({reverse_increm}, '{date_format}'));
                counter := counter + 1;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;

        DROP TABLE IF EXISTS date_index_{ui};
        CREATE TEMPORARY TABLE date_index_{ui}(date_str varchar(255));
        CALL create_index{ui}('{start_date}'::VARCHAR, '{end_date}'::VARCHAR);
        -- Need to drop procedure; random suffix means infinite duplication otherwise.
        DROP PROCEDURE create_index{ui}(startDate INOUT VARCHAR, endDate INOUT VARCHAR);
        """

    def create_month_index(self, set_start_to_01=False):
        return self.create_date_index(
            date_format="YYYY-MM",
            date_increment="ADD_MONTHS(startDate::DATE, counter)",
            reverse_increm="ADD_MONTHS(endDate::DATE, -counter)",
            set_start_to_01=set_start_to_01,
        )

    def create_quarter_index(self, set_start_to_01=False):
        return self.create_date_index(
            date_format='YYYY-"Q"Q',
            date_increment=(
                "DATE_TRUNC('quarter', "
                "DATEADD('month', counter * 3, startDate::DATE))"
            ),
            reverse_increm=(
                "DATE_TRUNC('quarter', "
                "DATEADD('month', -counter * 3, endDate::DATE))"
            ),
            set_start_to_01=set_start_to_01,
        )

    def handle_insert_packs(self, sql: str) -> str:
        double = False
        for i in ["full_select_insert", "skip_hx_select_inserts"]:
            if i in sql:
                double = True
        if double:
            sql = self.double_squigglies(sql)
            full_select_insert = (
                "{product_select}{h1_select}{location_select}{h2_select}"
                "{project_select}{h7_select}"
                "{act_select}{evt_select}{ag_select}{gender_select}{team_select}{sex_select}"
                "{year_select}{month_select}{week_select}{dow_select}{area_select}{ff_select}"
            )
            skip_hx_select_inserts = (
                "{product_select}{location_select}{project_select}"
                "{act_select}{evt_select}{ag_select}{gender_select}{team_select}{sex_select}"
                "{year_select}{month_select}{week_select}{dow_select}{area_select}{ff_select}"
            )
            sql = format_string_with_dict(
                sql,
                {
                    "full_select_insert": full_select_insert,
                    "skip_hx_select_inserts": skip_hx_select_inserts,
                },
            )
        return sql

    def double_squigglies(self, sql):
        # Define the pattern using regular expression
        pattern = r"\{([^}]+)\}"
        # Find all matches using re.findall()
        matches = re.findall(pattern, sql)
        # Exclude specified substrings
        excluded = ["full_select_insert", "skip_hx_select_inserts"]
        filtered_matches = set([match for match in matches if match not in excluded])
        print(f"filtered_matches: {filtered_matches}")
        # Replace { with {{ and } with }}
        processed_sql = sql
        for match in filtered_matches:
            processed_sql = processed_sql.replace(
                "{" + match + "}", "{{" + match + "}}"
            )
        return processed_sql

    def fill_sql_inserts(
        self, sql: str, replacements: "Optional[dict[str, str]]" = None
    ) -> str:
        sql = self.handle_insert_packs(sql)
        print(f"fill_sql_insert's by: {self.params.by}")
        defaults = {
            "ui": self.ui,
            "startDate": self.params.startDate,
            "endDate": self.params.endDate,
            "col": self.params.metric,
            "field_ref": self.params.field_ref,
            "by": self.params.by,
            "sby": self.params.sby,
            "target": self.params.target,
            "target_insert": self.target_insert,
            "product_select": self.params.ggie("product_type"),
            "h1_select": self.params.ggie("hierarchy1"),
            "h1_join": self.params.ggie("hierarchy1", "join"),
            "location_select": self.params.ggie("location"),
            "h2_select": self.params.ggie("hierarchy2"),
            "h2_join": self.params.ggie("hierarchy2", "join"),
            "project_select": self.params.ggie("project_id"),
            "h7_select": self.params.ggie("hierarchy7"),
            "act_select": self.params.ggie("activity_type"),
            "act_join": self.params.ggie("activity_type", "join"),
            "evt_select": self.params.ggie("event_type"),
            "ag_select": self.params.ggie("age_group"),
            "gender_select": self.params.ggie("gender"),
            "team_select": self.params.ggie("team"),
            "year_select": self.params.ggie("year"),
            "month_select": self.params.ggie("month"),
            "week_select": self.params.ggie("week"),
            "dow_select": self.params.ggie("day_of_week"),
            "area_select": self.params.ggie("area"),
            "sex_select": self.params.ggie("sex"),
            "ff_select": self.params.ggie("financial_firm"),
            "filter_wc": self.params.filters_string,
            "job_filter": self.params.job_filter,
            "table_name": getattr(self, "table_name", ""),
        }
        return format_string_with_dict(sql, defaults, replacements)


class MetricWTableName(MetricFromParameters):
    __slots__ = ["table_name"]

    def __init__(self, engine, ParameterClass, metric_dict, job_metric_dict={}):
        self.setTableName()
        super(MetricWTableName, self).__init__(
            engine, ParameterClass, metric_dict, job_metric_dict
        )

    @abstractmethod
    def setTableName(self):
        pass


class MetricBasic(MetricWTableName):
    """Subclass of MetricFromParameters for
    metrics without anything special going on."""

    @abstractmethod
    def setMetric(self):
        pass

    def setL1(self):
        self.setMetric()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setMetric()

    def setFullExport(self):
        self.setMetric()

    def setAllDates(self):
        self.setAllDatesSimple(self.table_name)

    def handleCSV(self, sql):
        sql = (
            sql
            + """
        SELECT * FROM base
        ;"""
        )
        df = self.make_df(sql)
        self.return_dict = df.to_csv(index=False)
        self.useS3("csv", use_json=True)
        return


class MetricXfactrTargetActual(MetricBasic):
    def setTableName(self):
        self.table_name = "stores_activated_xfctr"

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT  *
            FROM {table_name}
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}
            ORDER BY date
        )
        """
        if self.params.layer == "csv":
            sql = (
                sql
                + """
        SELECT *
        FROM base
        ORDER BY date
        ;"""
            )
            sql = self.fill_sql_inserts(sql)
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        sql = (
            sql
            + """
        , gc AS (
            SELECT *, 1 AS grouping_col
            FROM base
        )
        SELECT  SUM({col}) AS {col},
                SUM(target) as target{by}
        FROM gc
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        """
        )
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)


class MetricRiaCouponRedemption(MetricBasic):
    def setTableName(self):
        self.table_name = "coupon_redemption_ria01001"

    def setAllDatesSimple(self, table_name):
        sql = f"""
        SELECT DISTINCT TRUNC(date) AS date
        FROM {table_name}
        WHERE date IS NOT NULL{self.params.job_filter}
        ORDER BY date
        ;"""
        dts = pd.read_sql(sql, self.engine)
        print(sql)
        self.standardDatesProcessing(dts)

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT  *, 1 as grouping_col
            FROM {table_name}
            WHERE TRUE {filter_wc}
            ORDER BY date
        )
        """
        sql = sql.format(
            table_name=self.table_name,
            filter_wc=self.params.filters_string,
        )

        if self.params.layer == "csv":
            sql = (
                sql
                + """
        SELECT * FROM base ORDER BY date
        ;"""
            )
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return

        sql = (
            sql
            + """ 
        SELECT  SUM(coupon_redemption) AS {col}, COUNT(coupon_redemption) AS count{by}
        FROM base
        WHERE date >= '{startDate}'
            AND date <= '{endDate}'{filter_wc}
        GROUP BY grouping_col{by}
        """
        )
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)


class MetricBaseLiveLocationCount(MetricBasic):
    """Draw locations data for all tables into a CTE for a consistent scheme."""

    def setAllDatesSimple(self, table_name):
        sql = f"""
        SELECT DISTINCT date
        FROM {table_name}
        WHERE date IS NOT NULL{self.params.job_filter}
        ORDER BY date
        ;"""
        dts = pd.read_sql(sql, self.engine)
        print(sql)
        self.standardDatesProcessing(dts)

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT  *
            FROM {table_name}
            WHERE TRUE {job_filter}{filter_wc}
            ORDER BY date
        )
        """
        sql = sql.format(
            table_name=self.table_name,
            job_filter=self.params.job_filter,
            filter_wc=self.params.filters_string,
        )
        if self.params.layer == "csv":
            sql = (
                sql
                + """
        SELECT * FROM base ORDER BY date
        ;"""
            )
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        elif self.params.layer == "2":
            if self.params.by == "":
                sql = (
                    sql
                    + """
        SELECT COUNT(DISTINCT(live_location)) AS live_location,
               date::VARCHAR,
               1 AS rnk
        FROM base
        GROUP BY date
        ORDER BY date
        ;"""
                )
            else:
                sql = (
                    sql
                    + """
        , complete_by AS (
            SELECT  DISTINCT {sby},
                    DENSE_RANK () OVER (ORDER BY {sby}) AS rnk
            FROM base
        ), joined AS (
            SELECT  base.date, 
                    complete_by.{sby},
                    complete_by.rnk
            FROM base
            JOIN complete_by ON TRUE
            GROUP BY complete_by.{sby}, base.date, complete_by.rnk
        ), counted  AS (
          SELECT  COUNT(DISTINCT(live_location)) AS live_location,
                  b.date::VARCHAR,
                  {sby}
          FROM base b
          GROUP BY b.date, {sby}
          ORDER BY b.date, {sby}   
        )
        SELECT COALESCE(c.live_location,0) AS live_location,
                j.date::VARCHAR,
                j.rnk,
                j.{sby}
        FROM joined j
        LEFT JOIN counted c ON j.date = c.date AND j.{sby} = c.{sby}
        ORDER BY j.date, j.{sby}    
        ;"""
                )
            sql = sql.format(sby=self.params.sby)

        else:
            sql = """
            WITH l1 AS (
                SELECT  COALESCE(e.lat,e.lat) || ',' || 
                            COALESCE(e.lng, e.lng) AS live_location, 
                        j.number as project_id
                FROM events e
                JOIN teams t ON e.team_id = t.id
                JOIN jobs j ON j.id = t.job_id
                JOIN roles r ON e.id = r.event_id
                JOIN bookings b ON r.id = b.role_id
                JOIN staff_attendance sa ON sa.booking_id = b.id
                WHERE sa.check_in_time_actual IS NOT NULL
                    AND ((check_out_time_actual IS NULL) 
                          OR (check_out_time_actual > GETDATE()))
                    AND e.activity_date = CURRENT_DATE
                UNION
                
                SELECT  DISTINCT(work_location) AS live_location,
                        CASE WHEN jo.job_number = '' THEN jo.job_order_id::VARCHAR 
                             ELSE jo.job_number END AS project_id
                FROM jobs_nc j
                JOIN job_orders_nc jo ON j.job_order_id = jo.job_order_id
                JOIN timesheets_nc ts ON ts.job_id = j.job_id
                WHERE j.is_deleted = false
                    AND ts.check_in_time IS NOT NULL
                    AND (ts.check_out_time IS NULL) OR (ts.check_out_time > GETDATE())
                    AND TRUNC(j.from) = CURRENT_DATE
                UNION
                
                SELECT  COALESCE(jl.lat, jp.lat) || ',' || 
                            COALESCE(jl.lon, jp.lon) AS live_location,
                        p.id::VARCHAR as project_id
                FROM jobs_1crm_at j
                LEFT JOIN job_live_locations_1crm_at jl ON jl.job_id = j.id
                LEFT JOIN ( SELECT jpl.job_id, jpl.id, s.lat, s.lon
                            FROM job_pos_locations_1crm_at jpl
                            JOIN sites_1crm_at s ON jpl.site_id = s.id
                            WHERE jpl.deleted_at IS NULL
                               AND s.deleted_at IS NULL) jp ON jp.job_id = j.id
                JOIN projects_1crm_at p ON p.id = j.project_id
                JOIN dates_1crm_at d ON j.id = d.job_id
                JOIN assignments_1crm_at a ON a.date_id = d.id
                JOIN checkins_1crm_at c ON a.id = c.assignment_id
                WHERE live_location IS NOT NULL
                    AND TRUNC(d.appointed_at) = CURRENT_DATE
                    AND c.performed_at IS NOT NULL
                    AND c.finished_at IS NULL
                    AND c.deleted_at IS NULL
                    AND a.deleted_at IS NULL
                    AND d.deleted_at IS NULL
                    AND j.deleted_at IS NULL
                    AND p.deleted_at IS NULL
                    AND jl.deleted_at IS NULL

                UNION
                
                SELECT  COALESCE(jl.lat, jp.lat) || ',' || 
                            COALESCE(jl.lon, jp.lon) AS live_location,
                        p.id::VARCHAR as project_id
                FROM jobs_1crm_de j
                LEFT JOIN job_live_locations_1crm_de jl ON jl.job_id = j.id
                LEFT JOIN ( SELECT jpl.job_id, jpl.id, s.lat, s.lon
                            FROM job_pos_locations_1crm_at jpl
                            JOIN sites_1crm_at s ON jpl.site_id = s.id
                            WHERE jpl.deleted_at IS NULL
                               AND s.deleted_at IS NULL) jp ON jp.job_id = j.id
                JOIN projects_1crm_de p ON p.id = j.project_id
                JOIN dates_1crm_de d ON j.id = d.job_id
                JOIN assignments_1crm_de a ON a.date_id = d.id
                JOIN checkins_1crm_de c ON a.id = c.assignment_id
                WHERE live_location IS NOT NULL
                    AND TRUNC(d.appointed_at) = CURRENT_DATE
                    AND c.performed_at IS NOT NULL
                    AND c.finished_at IS NULL
                    AND c.deleted_at IS NULL
                    AND a.deleted_at IS NULL
                    AND d.deleted_at IS NULL
                    AND j.deleted_at IS NULL
                    AND p.deleted_at IS NULL
                    AND jl.deleted_at IS NULL
                    
                UNION
                
                SELECT  COALESCE(jl.lat, jp.lat) || ',' || 
                            COALESCE(jl.lon, jp.lon) AS live_location,
                        p.id::VARCHAR as project_id
                FROM jobs_1crm_fr j
                LEFT JOIN job_live_locations_1crm_fr jl ON jl.job_id = j.id
                LEFT JOIN ( SELECT jpl.job_id, jpl.id, s.lat, s.lon
                            FROM job_pos_locations_1crm_at jpl
                            JOIN sites_1crm_at s ON jpl.site_id = s.id
                            WHERE jpl.deleted_at IS NULL
                               AND s.deleted_at IS NULL) jp ON jp.job_id = j.id
                JOIN projects_1crm_fr p ON p.id = j.project_id
                JOIN dates_1crm_fr d ON j.id = d.job_id
                JOIN assignments_1crm_fr a ON a.date_id = d.id
                JOIN checkins_1crm_fr c ON a.id = c.assignment_id
                WHERE live_location IS NOT NULL
                    AND TRUNC(d.appointed_at) = CURRENT_DATE
                    AND c.performed_at IS NOT NULL
                    AND c.finished_at IS NULL
                    AND c.deleted_at IS NULL
                    AND a.deleted_at IS NULL
                    AND d.deleted_at IS NULL
                    AND j.deleted_at IS NULL
                    AND p.deleted_at IS NULL
                    AND jl.deleted_at IS NULL
            )
            SELECT  COUNT(live_location) AS live_location
            FROM l1
            WHERE TRUE {job_filter}
            """
            sql = sql.format(job_filter=self.params.job_filter)
        self.make_df(sql)

    def setTableName(self):
        self.table_name = "base_live_locations"


class MetricInLCRM(MetricFromParameters):
    """Subclass of MetricFromParameters for metrics
    that can be pulled from the legacy CRM."""

    __slots__ = []

    def setWeatherLocations(self):
        sql = f"""
        SELECT events.lat || ',' || events.lng AS latlng
        FROM events
        LEFT JOIN teams ON teams.id = events.team_id
        LEFT JOIN jobs ON teams.job_id = jobs.id
        WHERE TRUE
          AND activity_date = CURRENT_DATE{self.params.job_filter}
        ;"""
        self.make_df(sql)

    def setL1(self):
        self.setWeatherLocations()
        self.l1 = L1DateGetter(self)

    def setL2(self):
        return  # Never used since only L1 in use.

    def setFullExport(self):
        return  # Never used since only L1 in use.

    def setAllDates(self):
        return  # Never used since only L1 in use.


class MetricInCRMWeather(MetricFromParameters):
    """Subclass of MetricFromParameters for the weather metric (from any CRM)."""

    __slots__ = []

    def setWeatherLocationsLegacy(self):
        """Set lat/lng for projects in legacy db."""
        sql = f"""
        SELECT events.lat || ',' || events.lng AS latlng
        FROM events
        LEFT JOIN teams ON teams.id = events.team_id
        LEFT JOIN jobs ON teams.job_id = jobs.id
        WHERE activity_date = CURRENT_DATE{self.params.job_filter}
        ;"""
        self.make_df(sql)

    def setWeatherLocationsNC(self):
        """Set lat/lng for projects in NextCrew db."""
        sql = f"""
        SELECT j.zip_code AS latlng
        FROM jobs_nc j
        JOIN job_orders_nc jo ON j.job_order_id = jo.job_order_id
        WHERE j.from = CURRENT_DATE{self.params.job_filter}
          AND j.is_deleted = FALSE
        ;"""
        self.make_df(sql)

    def setWeatherLocations1CRM(self, suffix):
        """Set lat/lng for projects in 1crm db."""
        # Hardcoded to Berlin for now.
        # sql=f"""
        # SELECT  COALESCE(jl.lat, jp.lat) || ',' || COALESCE(jl.lon, jp.lon) AS latlng
        # FROM jobs_1crm_{suffix} j
        # LEFT JOIN job_live_locations_1crm_{suffix} jl ON jl.job_id = j.id
        # LEFT JOIN ( SELECT jpl.job_id, jpl.id, s.lat, s.lon
        #             FROM job_pos_locations_1crm_{suffix} jpl
        #             JOIN sites_1crm_{suffix} s ON jpl.site_id = s.id) jp
        #               ON jp.job_id = j.id
        # JOIN projects_1crm_{suffix} p ON p.id = j.project_id
        # JOIN dates_1crm_{suffix} d ON j.id = d.job_id
        # WHERE latlng IS NOT NULL
        #   AND d.appointed_at = CURRENT_DATE{self.params.job_filter}
        # ;"""
        # self.make_df(sql, to_self=False)
        df = pd.DataFrame({"latlng": "Berlin"})
        self.df = df

    def setWeatherLocations1CRMAT(self):
        """Set lat/lng for projects in Austrian 1crm db."""
        self.setWeatherLocations1CRM("at")

    def setWeatherLocations1CRMDE(self):
        """Set lat/lng for projects in German 1crm db."""
        self.setWeatherLocations1CRM("de")

    def setWeatherLocations1CRMFR(self):
        """Set lat/lng for projects in French 1crm db."""
        self.setWeatherLocations1CRM("fr")

    def setL1(self):
        """Set lat/lng depending on parameters."""
        job = self.params.job
        db = get_db(job, self.job_metric_dict)
        pick_weather_method_dict = {
            "legacy_uk": self.setWeatherLocationsLegacy,
            "nc_us": self.setWeatherLocationsNC,
            "1crm_at": self.setWeatherLocations1CRMAT,
            "1crm_de": self.setWeatherLocations1CRMDE,
            "1crm_fr": self.setWeatherLocations1CRMFR,
        }
        if isinstance(db, str):
            pick_weather_method_dict[db]()
        elif isinstance(db, list):
            collective_df = pd.DataFrame(columns=["latng"])
            for i in db:
                pick_weather_method_dict[i]()
                collective_df = pd.concat([collective_df, self.df], ignore_index=True)
            self.df = collective_df
        self.l1 = L1DateGetter(self)

    def setL2(self):
        """Ignore."""
        return  # Never used since only L1 in use.

    def setFullExport(self):
        """Ignore."""
        return  # Never used since only L1 in use.

    def setAllDates(self):
        """Ignore."""
        return  # Never used since only L1 in use.


# class MetricNSurveys(MetricFromParameters):
#     """ Subclass of MetricFromParameters for
#         metrics that can be pulled from the legacy CRM. """
#     __slots__ = []

#     def setNSurveys(self):
#         sql="""
#         SELECT SUM(nsurveys) AS nsurveys
#         FROM (  SELECT COUNT(*) AS nsurveys
#                 FROM fs_submissions_arc_2021_11_23
#                 UNION ALL
#                 SELECT COUNT(*) AS nsurveys
#                 FROM submissions
#                 WHERE sub_timestamp > '2021-11-23'
#                 UNION ALL
#                 SELECT COUNT(*) AS nsurveys
#                 FROM tf_responses)
#         ;"""
#         self.make_df(sql)

#     def setL1(self):
#         self.setNSurveys()
#         self.l1 = L1DateGetterRecords(self)

#     def setL2(self):
#         return # Never used since only L1 in use.
#     def setFullExport(self):
#         return # Never used since only L1 in use.
#     def setAllDates(self):
#         self.startDate = '2016-10-18' # str(dts[0])
#         self.endDate = datetime.utcnow().strftime('%Y-%m-%d') # str(dts[-1])
#         self.startAt = self.startDate # self.startDate
#         self.endAt = self.endDate # self.endDate
#         self.dateArray = []# da
#         return# Never used since only L1 in use.

# class MetricNConsumersEngaged(MetricFromParameters):
#     """ Subclass of MetricFromParameters for
#         metrics that can be pulled from the legacy CRM. """
#     __slots__ = []

#     def setNConsumersEngaged(self):
#         sql="""
#         SELECT  SUM(value)::INTEGER + 480000 AS consumers_engaged
#         FROM tf_response_answers
#         WHERE field_ref LIKE 'engagement_score%%'
#         ;"""
#         self.make_df(sql)

#     def setL1(self):
#         self.setNConsumersEngaged()
#         self.l1 = L1DateGetterRecords(self)

#     def setL2(self):
#         return # Never used since only L1 in use.
#     def setFullExport(self):
#         return # Never used since only L1 in use.
#     def setAllDates(self):
#         return# Never used since only L1 in use.


class MetricWithIndicators(MetricFromParameters):
    """Subclass of MetricFromParameters for metrics that might have indicators.
    With the use of self.l1, obsolete."""

    __slots__ = []


class MetricTFFormat(MetricWTableName):
    """Subclass of MetricWTableName for
    metrics that can be pulled directly from TypeForm."""

    def setAllDates(self):
        sql = f"""
        SELECT DISTINCT date::DATE AS date
        FROM {self.table_name} m
        """
        sql_suffix = f"""
        WHERE field_ref LIKE '{self.params.field_ref}%%'{self.params.job_filter}
        ORDER BY date
        ;"""
        if self.params.field_ref is None:
            sql_suffix = f"""
        WHERE TRUE{self.params.job_filter}
        ORDER BY date
        ;"""
        sql = sql + sql_suffix
        print(sql)
        dts = pd.read_sql(sql, self.engine)
        self.standardDatesProcessing(dts)

    @abstractmethod
    def setL1(self):
        pass

    @abstractmethod
    def setL2(self):
        pass

    def setFullExport(self):
        field_ref = ""
        if self.params.field_ref is not None:
            field_ref = f"  AND m.field_ref LIKE '{self.params.field_ref}%%'"
        sql = """
        SELECT  form_id, 
                response_id,
                field_ref,
                value,
                product_type::VARCHAR, 
                location,
                activity_type::VARCHAR,
                date,
                day_of_week,
                week,
                month,
                year
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        LEFT JOIN m.act AS activity_type ON TRUE
        WHERE TRUNC(m.date) >= '{startDate}'
          AND TRUNC(m.date) <= '{endDate}'
          {field_ref}{filter_wc}{job_filter}
        ORDER BY m.response_id
        ;"""
        sql = sql.format(
            col=self.params.metric,
            startDate=self.params.startDate,
            endDate=self.params.endDate,
            field_ref=field_ref,
            filter_wc=self.params.filters_string,
            job_filter=self.params.job_filter,
            table_name=self.table_name,
        )
        df = self.make_df(sql, to_self=False)
        self.return_dict = df.to_csv(index=False)
        self.useS3("csv", use_json=True)

    def genericAvgSum(self, agg, split=False, custom_dates=None):
        if agg == "avg":
            agg_str = f"ROUND(AVG(value::INTEGER * 1.0), 2) AS {self.params.metric}"
        elif agg == "sum":
            agg_str = f"SUM(value::INTEGER) AS {self.params.metric}"
        c_start = self.params.startDate
        c_end = self.params.endDate
        if custom_dates is not None:
            c_start = custom_dates["startDate"]
            c_end = custom_dates["endDate"]
        final_table_reference = "by_response"
        if split:  # Don't want to group at the response level because one response will
            final_table_reference = "base"  #  have multiple valid values.
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col,
                    field_ref,
                    form_id, 
                    m.response_id,
                    date, 
                    value{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE TRUNC(date) >= '{c_start}'
                AND TRUNC(date) <= '{c_end}'
                AND field_ref LIKE '{field_ref}%%'{filter_wc}{job_filter}
        ), by_response AS (
            SELECT  value, 
                    1 AS grouping_col, 
                    form_id, 
                    date, 
                    response_id{full_select_insert}
            FROM base
            GROUP BY form_id, field_ref, date, response_id, value{skip_hx_select_inserts}
        )
        SELECT  {agg_str}, COUNT(value) AS size{by}{target}
        FROM {final_table_reference} m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(
            sql,
            {
                "agg_str": agg_str,
                "final_table_reference": final_table_reference,
                "c_start": c_start,
                "c_end": c_end,
            },
        )
        self.make_df(sql)

    def setGenericAvg(self):
        self.genericAvgSum("avg")

    def setGenericSum(self):
        self.genericAvgSum("sum")

    def setGenericSplitSum(self):
        self.genericAvgSum("sum", True)

    def monthOverMonthAvgSum(self, agg):
        """Like genericAvgSum() but for L1s comparing month over month."""
        if agg == "avg":
            agg_str = f"AVG(value::INTEGER * 1.0) AS {self.params.metric}"
        elif agg == "sum":
            agg_str = f"SUM(value::INTEGER) AS {self.params.metric}"
        sql = (
            self.create_month_index()
            + """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    value, 
                    m.month{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE DATE_TRUNC('month', date) >= '{startDate}'
                AND DATE_TRUNC('month', date) <= '{endDate}'
                AND field_ref LIKE '{field_ref}%%'{filter_wc}{job_filter}
        )
        SELECT  {agg_str}, COUNT(value) AS size, d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN ( SELECT  grouping_col, form_id, response_id, 
                            MAX(value) AS value, month{by}
                    FROM base
                    GROUP BY grouping_col, form_id, response_id, month{by}) m 
               ON d.date_str = m.month
        GROUP BY d.date_str
        ORDER BY d.date_str
        LIMIT 2
        ;"""
        )
        # print(sql)
        by = self.params.by if self.params.by != ", month" else ""
        sql = self.fill_sql_inserts(
            sql, {"agg_str": agg_str, "by": by, "month_select": ""}
        )
        self.make_df(sql)

    def monthOverMonthAvg(self):
        self.monthOverMonthAvgSum("avg")

    def monthOverMonthSum(self):
        self.monthOverMonthAvgSum("sum")

    def setGallery(self):
        limit = ""
        if ("limit" in self.params.ai) and (int(self.params.ai["limit"]) > 0):
            limit = f"LIMIT {self.params.ai['limit']}"
        location_insert = "location,"
        if self.params.ggie("location") != "":
            # Use gggie rathr than gfie because
            # location is only included for groupings, not filters.
            location_insert = ""
        sql = """
        SELECT  date, location, value AS {col}
        FROM (  SELECT  1 AS grouping_col, 
                        form_id, 
                        m.response_id, 
                        value, 
                        {location_insert}
                        TO_CHAR(date, 'YYYY-MM-DD') AS date{full_select_insert}
                FROM {table_name} m{act_join}{h1_join}
                WHERE TRUNC(date) >= '{startDate}'
                  AND TRUNC(date) <= '{endDate}'
                  AND value != ''
                  AND field_ref LIKE '{field_ref}%%'{filter_wc}{job_filter}) m
        GROUP BY date, location, {col}
        ORDER BY date, location, {col}
        {limit}
        ;"""
        print(sql)
        sql = self.fill_sql_inserts(
            sql, {"limit": limit, "location_insert": location_insert}
        )
        self.make_df(sql)

    def setTFWeatherLocations(self, location_field):
        """Not in use, right?"""
        sql = """
        SELECT  COUNT(DISTINCT location) AS latlng
        FROM {table_name}
        WHERE date = CURRENT_DATE{job_filter};"""
        sql = sql.format(job_filter=self.params.job_filter, table_name=self.table_name)
        self.make_df(sql)

    def setCategoryCount(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    CASE WHEN value = '' THEN 'Blank' WHEN value IS NOT NULL 
                         THEN value ELSE 'Unanswered' END AS {col}{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'{filter_wc}{job_filter}
              AND field_ref LIKE '{field_ref}%%'
              AND value != ''
        )
        """
        l1_order = f"ORDER BY count{self.params.by}"
        if self.params.layer == "1":
            if len(self.params.l1_ref) > 0:
                l1_order = f"""ORDER BY (CASE WHEN {
                    self.params.metric} = '{
                    self.params.l1_ref}' THEN 1 ELSE 2 END), count"""
            sql = (
                sql
                + """
        SELECT {col}, count, size{by}
        FROM  ( SELECT {col}, COUNT(response_id) AS count{by}
                FROM base
                WHERE {col} NOT IN ('Blank', 'Unanswered')
                GROUP BY {col}{by} 
                ORDER BY count DESC
                -- LIMIT 1) # Drop in order to get indicators. 
                ) m
        JOIN  ( SELECT SUM(grouping_col) AS size
                FROM base ) s ON TRUE
        {l1_order}
        ;"""
            )
        elif self.params.layer == "2":
            sql = (
                sql
                + """
        SELECT {col}, count, size{sdotby}
        FROM  ( SELECT {col}, COUNT(response_id) AS count{by}
                FROM base 
                GROUP BY {col}{by} ) m
        JOIN  ( SELECT 1 AS dummy, SUM(grouping_col) AS size{by}
                FROM base
                GROUP BY dummy{by} ) s ON TRUE{byjoin}
        ORDER BY count{sdotby}
        ;"""
            )
        # print(sql)
        if self.params.sby == "":
            sdotby = ""
            byjoin = ""
        else:
            sdotby = f", s.{self.params.sby}"
            byjoin = f" AND s.{self.params.sby} = m.{self.params.sby}"
        sql = self.fill_sql_inserts(
            sql, {"sdotby": sdotby, "byjoin": byjoin, "l1_order": l1_order}
        )
        self.make_df(sql)

    def setSampleDemographic(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    m.value::INTEGER AS sampling_demographics_f,
                    sampling_demographics_m::INTEGER{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            LEFT JOIN ( SELECT  response_id,
                                value AS sampling_demographics_m
                        FROM {table_name}
                        WHERE field_ref LIKE 'sampling_demographics_m%%') dt 
                   ON dt.response_id = m.response_id
            WHERE TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'
                AND m.field_ref LIKE 'sampling_demographics_f%%'{filter_wc}{job_filter}
        )
        SELECT  'Female' AS sampling_demographics, 
                SUM(sampling_demographics_f) AS count,
                SUM(sampling_demographics_m) + SUM(sampling_demographics_f) AS size{by}
        FROM base
        GROUP BY grouping_col{by}
        UNION ALL
        SELECT  'Male' AS sampling_demographics, 
                SUM(sampling_demographics_m) AS count,
                SUM(sampling_demographics_m) + SUM(sampling_demographics_f) AS size{by}
        FROM base
        GROUP BY grouping_col{by}
        ;"""
        # print(sql)
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setSustainabilityScore(self):
        # See record_keeping_functions.py
        sql = """
        WITH base AS (
            SELECT  form_id, 
                    response_id, 
                    date,
                    value AS mode_of_transport{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE field_ref LIKE 'mode_of_transport%%'
                AND TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'{filter_wc}{job_filter}
        )
        SELECT  SUM(sustainability_score)::INTEGER AS sustainability_score, 
                COUNT(sustainability_score) AS size{by}{target}
        FROM (  SELECT  1 AS grouping_col, 
                        form_id, 
                        m.response_id, 
                        CASE WHEN distance_km IS NULL 
                             THEN 1.60934 * distance_miles::FLOAT 
                             ELSE distance_km::FLOAT END AS distance_in_km,
                        COALESCE(distance_in_km * ss.value, 0) AS distance_weighting,
                        COALESCE(1000* waste_management::INTEGER, 0) AS waste_weighting,
                        distance_weighting + waste_weighting AS sustainability_score, 
                        TO_CHAR(m.date, 'YYYY-MM-DD') AS date{by}
                FROM      ( SELECT  form_id, response_id, date, 
                                    MAX(mode_of_transport) AS mode_of_transport{by}
                            FROM base
                            GROUP BY form_id, response_id, date{by}) m
                LEFT JOIN ( SELECT  response_id, 
                                    value AS distance_km
                            FROM {table_name}
                            WHERE field_ref LIKE 'distance_km%%') km
                       ON km.response_id = m.response_id
                LEFT JOIN ( SELECT  response_id, 
                                    value AS distance_miles
                            FROM {table_name}
                            WHERE field_ref LIKE 'distance_miles%%') miles
                       ON miles.response_id = m.response_id
                LEFT JOIN ( SELECT  response_id, 
                                    value AS waste_management
                            FROM {table_name}
                            WHERE field_ref LIKE 'waste_management%%') wm ON
                       wm.response_id = m.response_id
                LEFT JOIN sustainability_scores ss 
                       ON ss.mode_of_transportation = m.mode_of_transport
            ) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        # print(sql)
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setDistanceKm(self):
        """Identical to .setGenericAvg() save for
        CASE WHEN distance_km IS NULL THEN 1.60934 * distance_miles::FLOAT
             ELSE distance_km::FLOAT END AS {col},
            line.  Maybe consolidate them?"""
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    CASE WHEN distance_km IS NULL THEN 1.60934 * distance_miles::FLOAT 
                         ELSE distance_km::FLOAT END AS {col}, 
                    TO_CHAR(m.date, 'YYYY-MM-DD') AS date{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            LEFT JOIN ( SELECT  response_id, 
                                value AS distance_km
                        FROM {table_name}
                        WHERE field_ref LIKE 'distance_km%%') km 
                   ON m.response_id = km.response_id
            LEFT JOIN ( SELECT  response_id, 
                                value AS distance_miles
                        FROM {table_name}
                        WHERE field_ref LIKE 'distance_miles%%') miles 
                   ON m.response_id = miles.response_id
            WHERE TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'
                AND m.field_ref LIKE 'mode_of_transportation%%'{filter_wc}{job_filter}
        )
        SELECT  AVG({col}::INTEGER * 1.0) AS {col}, COUNT({col}) AS size{by}{target}
        FROM (  SELECT grouping_col, form_id, response_id, MAX({col}) AS {col}{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id{by}) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def generate_perma_sql(self):
        sql = """
        WITH tfbase AS (
            SELECT  response_id,
                    form_id,
                    value,
                    field_ref{full_select_insert}
            FROM {table_name} m
            WHERE form_id != 'eKTPQQQp'
                AND TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'{filter_wc}{job_filter}
        ), grouped_by_response_id AS (
            SELECT response_id,
                    form_id,
                    MAX(value) AS value,
                    field_ref{by}
            FROM tfbase
            GROUP BY response_id, form_id, field_ref{by}
        ), nonpermkm AS (
            SELECT  response_id, 
                    value AS distance_km
            FROM grouped_by_response_id
            WHERE field_ref LIKE 'distance_km%%'
        ), nonpermmi AS (
            SELECT  response_id, 
                    value AS distance_miles
            FROM grouped_by_response_id
            WHERE field_ref LIKE 'distance_miles%%'
        ), nonpermmot AS (
            SELECT  response_id,
                    value AS mode_of_transport{by}
            FROM grouped_by_response_id
            WHERE field_ref LIKE 'mode_of_transportation%%'     
        ), nonpermcombined AS (
            SELECT  m.response_id,
                    CASE WHEN distance_km IS NULL THEN 1.60934 * distance_miles::FLOAT 
                         ELSE distance_km::FLOAT END AS distance_km,
                    mode_of_transport{by}
            FROM nonpermmot m
            LEFT JOIN nonpermkm k ON m.response_id = k.response_id
            LEFT JOIN nonpermmi i ON m.response_id = i.response_id
        ), perm AS (
            SELECT  1 AS grouping_col, 
                    distance_km,
                    mode_of_transport, 
                    TO_CHAR(m.date, 'YYYY-MM-DD') AS date{full_select_insert}
            FROM sus_field_staff_sustainability m
            WHERE TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'
                AND distance_km IS NOT NULL{filter_wc}
        ),        
        """
        return sql

    def setBaseKM100(self, agg):
        """"""
        metric = self.params.metric
        if agg == "journey":
            agg_str = f"""COUNT(CASE WHEN {metric} *2 > 100 THEN 1 END)"""
        elif agg == "distance":
            agg_str = f"""ROUND(SUM(GREATEST({metric}-100, 0)),2)"""
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    CASE WHEN distance_km IS NULL THEN 1.60934 * distance_miles::FLOAT 
                         ELSE distance_km::FLOAT END AS {col}, 
                    TO_CHAR(m.date, 'YYYY-MM-DD') AS date{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            LEFT JOIN ( SELECT  response_id, 
                                value AS distance_km
                        FROM {table_name}
                        WHERE field_ref LIKE 'distance_km%%') km 
                   ON m.response_id = km.response_id
            LEFT JOIN ( SELECT  response_id, 
                                value AS distance_miles
                        FROM {table_name}
                        WHERE field_ref LIKE 'distance_miles%%') miles 
                   ON m.response_id = miles.response_id
            WHERE TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'
                AND m.field_ref LIKE 'mode_of_transportation%%'{filter_wc}{job_filter}
        )
        SELECT  {agg_str} AS {col}, COUNT({col}) AS size{by}{target}
        FROM (  SELECT grouping_col, form_id, response_id, MAX({col}) AS {col}{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id{by}) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by} DESC
        ;"""
        # print(sql)
        sql = self.fill_sql_inserts(sql, {"agg_str": agg_str})
        self.make_df(sql)

    def setNetPromoterScore(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    CASE WHEN m.value IN (1, 2, 3, 4, 5, 6) THEN -1
                            WHEN m.value IN (7, 8) THEN 0
                            WHEN m.value IN (9, 10) THEN 1 
                            ELSE 0 END AS {col}{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'
                AND m.field_ref LIKE '{col}%%'{filter_wc}{job_filter}
        )
        SELECT  ROUND(SUM({col}) *100.0 / COUNT({col}))::INTEGER AS {col}, 
                COUNT({col}) AS size{by}{target}
        FROM (  SELECT grouping_col, form_id, response_id, MAX({col}) AS {col}{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id{by}) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        # print(sql)
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setResponseCount(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    m.form_id, 
                    m.response_id, 
                    m.date{full_select_insert}
            FROM    {table_name} m{act_join}{h1_join}
            WHERE TRUNC(date) >= '{startDate}'
                AND TRUNC(date) <= '{endDate}'{filter_wc}{job_filter}
        )
        SELECT  COUNT(DISTINCT m.response_id) AS {col}, 
                COUNT(DISTINCT m.response_id) AS size{by}{target}
        FROM (  SELECT grouping_col, form_id, response_id, date{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id, date{by}) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setAnswerCount(self):
        core = """
        WITH m AS (
            SELECT  form_id, 
                    response_id,
                    field_ref,
                    value,
                    date{by}
            FROM (  SELECT  form_id, 
                            response_id,
                            field_ref,
                            value,
                            date{full_select_insert}
                    FROM {table_name} m{act_join}{h1_join}
                    WHERE TRUNC(date) >= '{startDate}'
                    AND TRUNC(date) <= '{endDate}'{filter_wc}{job_filter})
            GROUP BY form_id, response_id, field_ref, value, date{by}
        )
        """
        core = self.fill_sql_inserts(core)
        sql = """
        SELECT  COUNT(m.response_id) AS {col}, 
                COUNT(DISTINCT m.response_id) AS size{by}{target}
        FROM (  SELECT 1 AS grouping_col, m.*
                FROM m) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        if self.params.layer == "csv":
            sql = (
                core
                + """
            SELECT * 
            FROM m
            ORDER BY date
            ;"""
            )
            df = self.make_df(sql)
            df["date"] = df["date"].dt.round("D")
            self.df = df
            self.return_dict = df.to_csv(index=False)
            return
        elif self.params.layer == "1":
            sql = (
                self.create_month_index()
                + core
                + """
        SELECT  COUNT(m.response_id) AS {col}, 
                COUNT(DISTINCT m.response_id) AS size,
                d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN m ON d.date_str = m.month
        GROUP BY date_str
        ORDER BY date_str 
        ;"""
            )
        else:
            sql = core + sql
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setFootfall(self):
        # PLACEHOLDER FOR ACTUALLY FINDING shift_multiplier
        shift_multiplier = 3
        ffh_insert = ""
        job = self.params.job
        if job != "":
            if "footfall_hours" in self.job_metric_dict[job]["footfall"]:
                ffh_insert = f" * 60 * {shift_multiplier}"
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    value::BIGINT{ffh_insert} AS footfall{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'
                AND field_ref LIKE 'footfall%%'{filter_wc}{job_filter}
        )
        SELECT  SUM(footfall) AS footfall, COUNT(footfall) AS size{by}{target}
        FROM (  SELECT grouping_col, form_id, response_id, MAX(footfall) AS footfall{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id{by}) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql, {"ffh_insert": ffh_insert})
        self.make_df(sql)

    def setEngagementScore(self, custom_dates=None):
        # PLACEHOLDER FOR ACTUALLY FINDING shift_multiplier
        shift_multiplier = 3
        ffh_insert = ""
        c_start = self.params.startDate
        c_end = self.params.endDate
        if custom_dates is not None:
            c_start = custom_dates["startDate"]
            c_end = custom_dates["endDate"]
        job = self.params.job
        if job != "":
            if "footfall" in self.job_metric_dict[job]:
                if "footfall_hours" in self.job_metric_dict[job]["footfall"]:
                    ffh_insert = f" * 60 * {shift_multiplier}"
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    m.value AS engagement_score,
                    footfall{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            LEFT JOIN ( SELECT  response_id,
                                value::BIGINT{ffh_insert} AS footfall
                        FROM {table_name}
                        WHERE field_ref LIKE 'footfall%%') f 
                   ON f.response_id = m.response_id
            WHERE TRUNC(m.date) >= '{c_start}'
                AND TRUNC(m.date) <= '{c_end}'
                AND m.field_ref LIKE 'engagement_score%%'{filter_wc}{job_filter}
        )
        SELECT  COUNT(engagement_score) AS size,
                SUM(footfall) AS total_footfall,
                SUM(engagement_score) AS total_engagements,
                CASE WHEN total_footfall > 0 
                     THEN ROUND(100.0 * total_engagements / total_footfall, 1) 
                     ELSE NULL END AS engagement_score{by}{target} 
        FROM (  SELECT  grouping_col, form_id, response_id, 
                        MAX(engagement_score) AS engagement_score, 
                        MAX(footfall) AS footfall{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id{by}) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(
            sql, {"ffh_insert": ffh_insert, "c_start": c_start, "c_end": c_end}
        )
        self.make_df(sql)

    def setEngagementScoreMonthOverMonth(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    m.value AS engagement_score,
                    footfall,
                    month{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            LEFT JOIN ( SELECT  response_id,
                                value::INTEGER AS footfall
                        FROM {table_name}
                        WHERE field_ref LIKE 'footfall%%') f 
                   ON f.response_id = m.response_id
            WHERE DATE_TRUNC('month', m.date) >= '{startDate}'
                AND DATE_TRUNC('month', m.date) <= '{endDate}'
                AND m.field_ref LIKE 'engagement_score%%'{filter_wc}{job_filter}
        )
        SELECT  COUNT(engagement_score) AS size,
                SUM(footfall) AS total_footfall,
                SUM(engagement_score) AS total_engagements,
                CASE WHEN total_footfall > 0 
                     THEN ROUND(100.0 * total_engagements / total_footfall, 1) 
                     ELSE NULL END AS engagement_score{by}{target} 
        FROM (  SELECT  grouping_col, form_id, response_id, 
                        MAX(engagement_score) AS engagement_score, 
                        MAX(footfall) AS footfall, month{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id, month{by}) m
        GROUP BY month{by}
        ORDER BY month{by}
        ;"""
        by = self.params.by if self.params.by != ", month" else ""
        sql = self.fill_sql_inserts(sql, {"by": by, "month_select": ""})
        self.make_df(sql)

    def setPeopleEngaged(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    m.value::INTEGER AS engagement_score,
                    daily_transaction::INTEGER,
                    month{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            LEFT JOIN ( SELECT  response_id,
                                value AS daily_transaction
                        FROM {table_name}
                        WHERE field_ref LIKE 'daily_transaction%%') dt
                   ON dt.response_id = m.response_id
            WHERE TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'
                AND m.field_ref LIKE 'engagement_score%%'{filter_wc}{job_filter}
        )
        SELECT  AVG(CASE WHEN engagement_score > 0 
                         THEN (daily_transaction * 100.00 / engagement_score) 
                         ELSE NULL END) AS people_engaged, 
                COUNT(engagement_score) AS size{by}{target}
        FROM (  SELECT  grouping_col, form_id, response_id, 
                        engagement_score, daily_transaction, month{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id, 
                         engagement_score, daily_transaction, month{by}) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def metricOverEngagement(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    form_id, 
                    m.response_id, 
                    m.value::INTEGER AS engagement_score,
                    {col}::INTEGER,
                    month{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            LEFT JOIN ( SELECT  response_id,
                                value AS {col}
                        FROM {table_name}
                        WHERE field_ref LIKE '{col}%%') dt 
                   ON dt.response_id = m.response_id
            WHERE TRUNC(m.date) >= '{startDate}'
                AND TRUNC(m.date) <= '{endDate}'
                AND m.field_ref LIKE 'engagement_score%%'{filter_wc}{job_filter}
        )
        SELECT  AVG(CASE WHEN engagement_score > 0 
                         THEN ROUND(({col}* 100.00/ engagement_score),2) 
                         ELSE NULL END) AS {col},
                COUNT(engagement_score) AS size{by}{target}
        FROM (  SELECT  grouping_col, form_id, response_id, 
                        engagement_score, {col}, month{by}
                FROM base
                GROUP BY grouping_col, form_id, response_id, 
                         engagement_score, {col}, month{by}) m
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setCategoryListCount(self):
        sql = """
        WITH base AS (
            SELECT  form_id, 
                    m.response_id,
                    TRIM(' "[]' FROM SPLIT_PART(m.value, ',', NS.n)) AS {col},
                    1 AS grouping_col{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            JOIN ns ON ns.n <= REGEXP_COUNT(m.value, ',') + 1
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'{filter_wc}{job_filter}
              AND field_ref LIKE '{field_ref}%%'
              AND value != ''
        )
        """
        limit_insert = ""
        if self.params.truncate:
            limit_insert = f"LIMIT {self.params.truncate}"
        if self.params.layer == "1":
            sql = (
                sql
                + """
        SELECT {col}, count, size{by}
        FROM  ( SELECT {col}, COUNT(response_id) AS count{by}
                FROM base
                WHERE {col} NOT IN ('Blank', 'Unanswered')
                GROUP BY {col}{by} 
                ORDER BY count DESC
                --LIMIT 1) # Must be dropped in order to get indicator.
                ) m
        JOIN  ( SELECT SUM(grouping_col) AS size
                FROM base ) s ON TRUE
        ORDER BY count{by}
        {limit_insert};"""
            )
        elif self.params.layer == "2":
            sql = (
                sql
                + """
        SELECT {col}, count, size{sdotby}
        FROM  ( SELECT {col}, COUNT(response_id) AS count{by}
                FROM base
                GROUP BY {col}{by} ) m
        JOIN  ( SELECT 1 AS dummy, SUM(grouping_col) AS size{by}
                FROM base
                GROUP BY dummy{by} ) s ON TRUE{byjoin}
        ORDER BY count{sdotby}, {col}
        {limit_insert};"""
            )
        if self.params.sby == "":
            sdotby = ""
            byjoin = ""
        else:
            sdotby = f", s.{self.params.sby}"
            byjoin = f" AND s.{self.params.sby} = m.{self.params.sby}"
        sql = self.fill_sql_inserts(
            sql, {"sdotby": sdotby, "byjoin": byjoin, "limit_insert": limit_insert}
        )
        self.make_df(sql)

    def getStockLevels(self):
        """ """
        params = self.params
        max_stock = self.job_metric_dict[params.job][params.metric]["max_stock"]
        by = ""
        if params.by != ", location":
            by = params.by
        sos_id = jnInserter(getParameterFromJMD("sos_id", params))
        sql = """
        WITH st AS (
            SELECT 
                REGEXP_REPLACE(hidden, ('(?<![a-zA-Z])'||CHR(39)||'|'||CHR(39)||
                    '(?![a-zA-Z])'), CHR(34), 1, 'p') AS rehidden,
                JSON_EXTRACT_PATH_TEXT(rehidden, 'event_id', TRUE) AS e_i,
                tr.response_id,
                m.value as start_stock
            FROM tf_responses tr
            JOIN tf_queries m ON m.response_id = tr.response_id
            WHERE m.field_ref LIKE 'start_stock%%'
                AND m.form_id IN ({sos_id})
        ), en AS (
            SELECT
                tr.response_id,
                m.value AS end_stock, 
                REGEXP_REPLACE(hidden, ('(?<![a-zA-Z])'||CHR(39)||'|'||CHR(39)||
                    '(?![a-zA-Z])'), CHR(34), 1, 'p') AS rehidden,
                JSON_EXTRACT_PATH_TEXT(rehidden, 'event_id', TRUE) AS e_i,
                TRUNC(m.date) AS date, 
                location{dow_select}{week_select}
            FROM tf_responses tr
            JOIN tf_queries m ON m.response_id = tr.response_id
            WHERE m.field_ref LIKE 'end_stock%%'{job_filter}
        ), base AS (
            SELECT  start_stock,  
                    end_stock,
                    st.e_i,
                    en.date,
                    {max_stock} as max_stock,
                    ((end_stock <= ({max_stock} * .2))::SMALLINT) AS resupply,
                    location{h2_select}{dow_select}{week_select}
            FROM st
            JOIN en ON en.e_i = st.e_i
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}
        )
        """
        sql = self.fill_sql_inserts(sql, {"max_stock": max_stock, "sos_id": sos_id})
        if params.layer == "csv":
            sql = sql + """SELECT * FROM base ORDER BY date;"""
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        if params.layer == "1":
            sql = (
                sql
                + f"""
        SELECT  SUM(resupply) AS resupply,
                SUM(end_stock) / {max_stock} * COUNT(location) * 100 AS stock_level, 
                LISTAGG(DISTINCT CASE WHEN base.resupply = 1 THEN location END, ', ') 
                    WITHIN GROUP (ORDER BY location) AS resupply_locations 
        FROM base
        """
            )
        elif params.layer == "2":
            sql = (
                sql
                + f""", 
        l2Values AS (
            SELECT  SUM(start_stock::INTEGER) - SUM(end_stock::INTEGER) 
                        AS stock_distributed,
                    SUM(end_stock) AS end_stock,
                    base.date::VARCHAR{by}
            FROM base
            GROUP BY date{by}
        )
        SELECT  ROUND(stock_distributed,2) AS stock_level,
                'stock_distributed' AS category_name,
                date, 
                2 AS rnk{by}
        FROM l2Values
        UNION
        SELECT  ROUND(end_stock::INTEGER,2) AS stock_level,
                'end_stock' AS category_name,
                date, 
                1 AS rnk{by}
        FROM l2Values
        ORDER BY date, rnk{by}
        """
            )
        self.make_df(sql)

    def setSamplesPerEngagement(self):
        sql = """
        WITH filtered AS (
            SELECT  form_id, response_id, date, field_ref, 
                    value{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'{filter_wc}{job_filter}
        ), e AS (
            SELECT form_id, response_id, date, value::INTEGER AS engagement_score{by}
            FROM filtered 
            WHERE field_ref LIKE ('engagement_score%%')
        ), d AS (
            SELECT value::INTEGER AS daily_transaction, response_id
            FROM filtered 
            WHERE field_ref LIKE ('daily_transaction%%')
        ), base AS (
            SELECT e.*, daily_transaction
            FROM d
            JOIN e ON d.response_id = e.response_id
        )
        """
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            sql = (
                sql
                + """
        SELECT  *, 
                ROUND(daily_transaction * 1.0 / CASE WHEN engagement_score = 0 
                    THEN 1 ELSE engagement_score END, 6) AS samples_per_engagement
        FROM base 
        ORDER BY date;
        """
            )
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        if self.params.by == "":
            sql = (
                sql
                + """
        SELECT  ROUND(1.0 * SUM(daily_transaction) / SUM(engagement_score) *100, 2) 
                    AS samples_per_engagement,
                COUNT(1) AS size
        FROM base
        """
            )
        elif self.params.layer == "2":
            sql = (
                sql
                + """
        SELECT  ROUND(1.0 * SUM(daily_transaction) / SUM(engagement_score) * 100, 2) 
                    AS samples_per_engagement{by},
                COUNT(1) AS size
        FROM base
        GROUP BY {sby}
        ORDER BY {sby}
        """
            )
            sql = sql.format(by=self.params.by, sby=self.params.sby)
        self.make_df(sql)

    def setLeadsConverted(self):
        sql = """
        WITH filtered AS (
            SELECT  form_id, response_id, date, field_ref, 
                    value{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'{filter_wc}{job_filter}
        ), e AS (
            SELECT  form_id, response_id, date, 
                    SUM(value)::INTEGER AS leads_generated{by}
            FROM filtered 
            WHERE field_ref LIKE ('leads_generated%%')
            GROUP BY response_id, form_id, date{by}
        ), d AS (
            SELECT SUM(value)::INTEGER AS daily_transaction, response_id
            FROM filtered 
            WHERE field_ref LIKE ('daily_transaction%%')
            GROUP BY response_id, form_id
        ), base AS (
            SELECT e.*, daily_transaction 
            FROM d
            JOIN e ON d.response_id = e.response_id
        )
        """
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            sql = (
                sql
                + """
        SELECT  *, 
                ROUND(leads_generated * 1.0 / CASE WHEN daily_transaction = 0 
                        THEN 1 ELSE daily_transaction END, 6) AS lead_conversion
        FROM base 
        ORDER BY date;
        """
            )
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        elif self.params.by == "":
            sql = (
                sql
                + """
        SELECT  ROUND(1.0 * SUM(leads_generated) / SUM(daily_transaction) * 100, 2) 
                    AS lead_conversion,
                COUNT(1) AS size
        FROM base
        """
            )
        elif self.params.layer == "1":
            sql = (
                sql
                + """
        SELECT  ROUND(1.0 * SUM(leads_generated) / SUM(daily_transaction) * 100, 2) 
                    AS lead_conversion,
                COUNT(1) AS size
        FROM base
        """
            )
        else:
            sql = (
                sql
                + """
        SELECT  ROUND(1.0 * SUM(leads_generated) / SUM(daily_transaction) * 100, 2) 
                    AS lead_conversion{by},
                COUNT(1) AS size
        FROM base
        GROUP BY {sby}
        ORDER BY {sby}
        """
            )
            sql = sql.format(by=self.params.by, sby=self.params.sby)
        self.make_df(sql)

    def set_highlighted_products(self):
        sql = """
        WITH filtered AS (
            SELECT  form_id, response_id, date, field_ref, 
                    value{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'
              AND field_ref LIKE '{field_ref}%%'{filter_wc}{job_filter}
        )
        """
        c_by_join = ""
        s_by_join = ""
        jby = ""
        if self.params.by != "":
            sby = self.params.sby
            c_by_join = f" AND j.{sby} = c.{sby}"
            s_by_join = f" AND j.{sby} = s.{sby}"
            jby = f", j.{sby}"
        if self.params.layer == "1":
            sql = (
                sql
                + """
        , prim AS (
            SELECT  *
            FROM filtered
            WHERE field_ref = 'highlighted_products_primary'
        ), nones AS (
            SELECT  response_id,
                    CASE WHEN value = '["None"]' THEN 1 ELSE 0 END AS none_values,
                    CASE WHEN value = '["None"]' THEN 0 ELSE 1 END AS non_nones
            FROM prim
        )
        SELECT  SUM(none_values) AS nones,
                SUM(non_nones) AS others,
                ROUND(100.0 * (1 - 1.0 * nones / others), 1) AS {col},
                '{startDate} :: {endDate}' AS date_range
        FROM nones
        ;"""
            )
        elif self.params.layer == "2":
            sql = (
                sql
                + """
        , size AS (
            SELECT  COUNT(response_id) AS responses, field_ref{by}
            FROM filtered
            GROUP BY field_ref{by}
        ), arr AS (
            SELECT  SPLIT_TO_ARRAY(TRIM('[]' FROM REPLACE(value, '"', '')), ',') AS arr,
                    field_ref{by}
            FROM filtered
        ), joined AS (
            SELECT  TRIM(BOTH '"' FROM value::VARCHAR) AS value,
                    field_ref{by} 
            FROM arr AS a
            JOIN a.arr AS value ON TRUE
        ), cnt AS (
            SELECT  COUNT(value) AS answers, field_ref{by}
            FROM joined
            GROUP BY field_ref{by}
        )
        SELECT  COUNT(j.*) AS cnt,
                j.value,
                DENSE_RANK () OVER ( PARTITION BY j.field_ref ORDER BY j.value ) AS rnk,
                SUBSTRING(j.field_ref FROM 22) || rnk AS stack,
                value || ': ' || cnt || ' of ' || s.responses || ' responses (' || 
                    ROUND(100.0 * cnt / s.responses, 1) || CHR(37) || ') and ' || c.answers || ' answers (' ||
                    ROUND(100.0 * cnt / c.answers, 1) || CHR(37) || ')' AS label{jby}
        FROM joined j
        JOIN cnt c ON j.field_ref = c.field_ref{c_by_join}
        JOIN size s ON j.field_ref = s.field_ref{s_by_join}
        GROUP BY j.value, j.field_ref, c.answers, s.responses{jby}
        ORDER BY stack, rnk{jby}
        ;"""
            )
        sql = self.fill_sql_inserts(
            sql, {"c_by_join": c_by_join, "s_by_join": s_by_join, "jby": jby}
        )
        self.make_df(sql)


class MetricTF(MetricTFFormat):
    """Subclass of MetricTFFormat for
    metrics that can be pulled directly from TypeForm."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemo(MetricTFFormat):
    """Subclass of MetricTFFormat for metrics that are
    formatted like TypeForm but are actually made up demos."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


# class MetricTFMP(MetricTF):
#     '''
#     '''
#     __slots__ = []
#     def setL1(self):
#         self.setMPSum()
#         self.l1 = L1IndicatorGetter(self)

#     def setL2(self):
#         self.setMPSum()


class MetricTFOverEngagement(MetricTFFormat):
    __slots__ = []

    def setL1(self):
        self.metricOverEngagement()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.metricOverEngagement()

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFFormatSum(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that can be pulled directly from TypeForm and get summed."""

    __slots__ = []

    def setL1(self):
        self.setGenericSum()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setGenericSum()


class MetricTFFormatSplitSum(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that can be pulled directly from TypeForm and get summed."""

    __slots__ = []

    def setL1(self):
        self.setGenericSplitSum()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setGenericSplitSum()


class MetricTFSum(MetricTFFormatSum):
    """Subclass of MetricTFFormat for metrics
    that can be pulled directly from TypeForm and get summed."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFSumWeather(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that can be pulled directly from TypeForm, get summed,
    and have weather overlays."""

    __slots__ = []

    def setL1(self):
        self.setGenericSum()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setGenericSum()
        self.l2_overlay = L2OverlayWeather(self)
        self.df = self.l2_overlay.set()

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFSplitSum(MetricTFFormatSplitSum):
    """Subclass of MetricTFFormat for metrics
    that can be pulled directly from TypeForm and get summed."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoSum(MetricTFFormatSum):
    """Subclass of MetricTFFormat for metrics that are formatted like TypeForm
    but are actually made up demos and get summed."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFFormatAvg(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that can be pulled directly from TypeForm and get averaged."""

    __slots__ = []

    def setL1(self):
        self.setGenericAvg()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setGenericAvg()


class MetricTFAvg(MetricTFFormatAvg):
    """Subclass of MetricTFFormat for metrics
    that can be pulled directly from TypeForm and get averaged."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoAvg(MetricTFFormatAvg):
    """Subclass of MetricTFFormat for metrics that are formatted like TypeForm
    but are actually made up demos and get averaged."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFAvgWeather(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that can be pulled directly from TypeForm, get averaged,
    and have weather overlays."""

    __slots__ = []

    def setL1(self):
        self.setGenericAvg()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setGenericAvg()
        self.l2_overlay = L2OverlayWeather(self)
        self.df = self.l2_overlay.set()

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFAvgMoM(MetricTFAvg):
    """Subclass of MetricTFAvg with L1 comparing month over month."""

    __slots__ = []

    def setL1(self):
        self.monthOverMonthAvg()
        self.l1 = L1IndicatorGetter(self)


class MetricTFSumMoM(MetricTFSum):
    """Subclass of MetricTFAvg with L1 comparing month over month."""

    __slots__ = []

    def setL1(self):
        self.monthOverMonthSum()
        self.l1 = L1IndicatorGetter(self)


class MetricTFAvgMin(MetricTFAvg):
    """Subclass of MetricTFAvg (and thus also MetricFromParameters)
    for engagement_length; only change from MetricTFAvg is adding 'min.'
    to the end of L1."""

    __slots__ = []

    def setL1(self):
        self.setGenericAvg()
        self.l1 = L1IndicatorGetter(self, metric_type_suffix=" min.")
        print(self.l1)


class MetricTFDemoAvgMin(MetricTFDemoAvg):
    """Subclass of MetricTFDemoAvg (and thus also MetricFromParameters)
    for engagement_length; only change from MetricTFDemoAvg is adding 'min.'
    to the end of L1."""

    __slots__ = []

    def setL1(self):
        self.setGenericAvg()
        self.l1 = L1IndicatorGetter(self, metric_type_suffix=" min.")
        print(self.l1)


class MetricTFAvgMinMoM(MetricTFAvgMin):
    """Subclass of MetricTFAvgMin for engagement_length;
    only change from MetricTFAvgMin is using monthOverMonthAvg for L1."""

    __slots__ = []

    def setL1(self):
        self.monthOverMonthAvg()
        self.l1 = L1IndicatorGetter(self, metric_type_suffix=" min.")
        print(self.l1)


# class MetricTFGalLocation(MetricTF):
#     """ Subclass of MetricTF (and thus also MetricFromParameters)
#         for metrics that can be pulled directly from TypeForm,
#         get displayed in a gallery and use `location` as location_field. """
#     __slots__ = []

#     def setL1(self):
#         return

#     def setL2(self):
#         self.setGallery('location')


class MetricTFFormatGalH2(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for metrics that can be pulled directly from TypeForm,
    get displayed in a gallery and use `hierarchy2` as location_field."""

    __slots__ = []

    def setL1(self):
        return  # No L1 for this kind of metric.

    def setL2(self):
        self.setGallery()

    def setFullExport(self):
        limit = ""
        if ("limit" in self.params.ai) and (int(self.params.ai["limit"]) > 0):
            limit = f"LIMIT {self.params.ai['limit']}"
        sql = """
        SELECT  form_id, 
                response_id,
                field_ref,
                value,
                product_type::VARCHAR, 
                location,
                activity_type::VARCHAR,
                TO_CHAR(date, 'YYYY-MM-DD') AS dt,
                day_of_week,
                week,
                month,
                year
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        LEFT JOIN m.act AS activity_type ON TRUE
        WHERE TRUNC(m.date) >= '{startDate}'
          AND TRUNC(m.date) <= '{endDate}'
          AND m.field_ref LIKE '{col}%%'{filter_wc}{job_filter}
          AND value != ''
        ORDER BY dt, location, value
        {limit}
        ;"""
        sql = self.fill_sql_inserts(sql, {"limit": limit})
        df = self.make_df(sql, to_self=False)
        self.return_dict = df.to_csv(index=False)
        self.useS3("csv", use_json=True)


class MetricTFGalH2(MetricTFFormatGalH2):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for metrics that can be pulled directly from TypeForm
    and get displayed in a gallery and use `hierarchy2` as location_field."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoGalH2(MetricTFFormatGalH2):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for metrics that can be pulled directly from TypeForm
    but are actually made up demos, get displayed in a gallery,
    and use `hierarchy2` as location_field."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFWeather(MetricTF):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for pulling location-data from TypeForm for the weather API.
    This version uses `hierarchy2` as field_ref."""

    __slots__ = []

    def setL1(self):
        self.setWeatherLocations("hierarchy2")

    def setL2(self):
        return  # No L2 for this kind of metric.


class MetricTFFormatCategoryCount(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for metrics that can be pulled directly from TypeForm
    and get displayed as counts."""

    __slots__ = []

    def setL1(self):
        self.setCategoryCount()
        boolmap = {True: "True", False: "False"}
        col = self.params.metric
        df = self.df.replace({col: boolmap})
        # print(df[col][0])
        # total=df['size'][0]
        df = df[(df[col] != "Blank") & (df[col] != "Unanswered")]
        if len(df.index) > 0:
            if df[col][0] in ["Not applicable", "Not Applicable"]:
                df[col] = df[col].str.replace(df[col][0], "N/A")
            else:
                df[col] = df[col].str.replace("seconds", "s")
                df[col] = df[col].str.replace("minutes", "m")
                df[col] = df[col].str.replace(" seconds", "s")
                df[col] = df[col].str.replace(" minutes", "m")
        print(df)
        self.df = df
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setCategoryCount()


class MetricTFCategoryCount(MetricTFFormatCategoryCount):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for metrics that get pulled as lists from TypeForm
    and get displayed as counts."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoCategoryCount(MetricTFFormatCategoryCount):
    """Subclass of MetricTFDemo (and thus also MetricFromParameters)
    for metrics that get pulled as lists from TypeForm-based demo data
    and get displayed as counts."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFSpecificCategory(MetricTF):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for metrics that can be pulled directly from TypeForm
    and get displayed as counts."""

    __slots__ = []

    def filter_for_l1_refs(self, col):
        df = self.df
        l1_refs = self.params.l1_refs
        if isinstance(l1_refs, str):  # convert single string to list for consistency
            l1_refs = [l1_refs]

        # Check if any of the values in l1_refs is in df[col].values
        if any(ref_val in df[col].values for ref_val in l1_refs):
            # Filter rows where the column value matches any value in l1_refs
            df = df[df[col].isin(l1_refs)]
            total_count = df["count"].sum()  # Sum the counts of filtered rows

            # Create new dataframe with the summed count
            df = df.head(1)
            df[col] = total_count
            df.drop(columns=["count"], inplace=True)
        else:
            df = df.head(1)
            df[col] = 0
            df = df[[col, "size"]]
        self.df = df

    def setL1(self):
        self.setCategoryCount()
        self.filter_for_l1_refs(self.params.metric)
        self.l1 = L1IndicatorGetter(self)

    def set_metric(self):
        sql = """
        WITH reasons AS (
            SELECT  response_id,
                    value
            FROM tf_queries m
            WHERE TRUNC(date) >= '{startDate}'
            AND TRUNC(date) <= '{endDate}'
            AND field_ref = 'non_activation_reason'{filter_wc}{job_filter}
        ), other_reasons AS (
            SELECT  response_id,
                    value
            FROM tf_queries m
            WHERE TRUNC(date) >= '{startDate}'
            AND TRUNC(date) <= '{endDate}'
            AND field_ref = 'non_activation_other_reason'{filter_wc}{job_filter}
        ), non_activation AS (
            SELECT  response_id,
                    TRUNC(date)::VARCHAR AS date,
                    location
            FROM tf_queries m
            WHERE TRUNC(date) >= '{startDate}'
            AND value IN ('False', '0')
            AND TRUNC(date) <= '{endDate}'
            AND field_ref LIKE '{field_ref}%%'{filter_wc}{job_filter}
        )
        SELECT  n.response_id,
                n.date,
                n.location,
                CASE WHEN o.value IS NOT NULL THEN r.value || ': ' || o.value
                     ELSE r.value END AS reason
        FROM non_activation n
        JOIN reasons r ON n.response_id = r.response_id
        LEFT JOIN other_reasons o ON n.response_id = o.response_id
        ;
        """
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setL2(self):
        self.set_metric()


class MetricTFSustainabilityScore(MetricTF):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating `sustainability_score` from TypeForm metrics."""

    __slots__ = []

    def setL1(self):
        self.setSustainabilityScore()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setSustainabilityScore()


class MetricTFFormatDistanceKm(MetricTFFormat):
    """Subclass of MetricTFFormat (and thus also MetricFromParameters)
    for calculating `distance_km` from TypeForm data."""

    __slots__ = []

    def setL1(self):
        self.setDistanceKm()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setDistanceKm()


class MetricTFDistanceKm(MetricTFFormatDistanceKm):
    """Subclass of MetricTFFormat (and thus also MetricFromParameters)
    for calculating `distance_km` from TypeForm data."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoDistanceKm(MetricTFFormatDistanceKm):
    """Subclass of MetricTFDemo (and thus also MetricFromParameters)
    for calculating `distance_km` from TypeForm-based demo data."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFJourneyOver100KM(MetricTF):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating `fs_100` from TypeForm metrics."""

    __slots__ = []

    def setL1(self):
        self.setBaseKM100("journey")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setBaseKM100("journey")


class MetricTFDistanceOver100KM(MetricTF):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating `fs_100` from TypeForm metrics."""

    __slots__ = []

    def setL1(self):
        self.setBaseKM100("distance")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setBaseKM100("distance")


class MetricTFFormatNPS(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating `net_promoter_score` from TypeForm."""

    __slots__ = []

    def setL1(self):
        self.setNetPromoterScore()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setNetPromoterScore()


class MetricTFNPS(MetricTFFormatNPS):
    """Subclass of MetricTFFormat (and thus also MetricFromParameters)
    for calculating `net_promoter_score` from TypeForm."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoNPS(MetricTFFormatNPS):
    """Subclass of MetricTFDemo (and thus also MetricFromParameters)
    for calculating `net_promoter_score` from TypeForm-based demo data."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFResponseCount(MetricTF):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating number of responses from TypeForm."""

    __slots__ = []

    def setL1(self):
        self.setResponseCount()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setResponseCount()


class MetricTFAnswerCount(MetricTF):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating number of responses from TypeForm."""

    __slots__ = []

    def setL1(self):
        self.setAnswerCount()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setAnswerCount()

    def setFullExport(self):
        self.setAnswerCount()


class MetricTFFormatFootfall(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating `footfall` from TypeForm."""

    __slots__ = []

    def setL1(self):
        self.setFootfall()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setFootfall()
        self.l2_overlay = L2OverlayWeather(self)
        self.df = self.l2_overlay.set()


class MetricTFFootfall(MetricTFFormatFootfall):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating `footfall` from TypeForm."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoFootfall(MetricTFFormatFootfall):
    """Subclass of MetricTFDemo (and thus also MetricFromParameters)
    for calculating `footfall` from TypeForm-based demo data."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFFormatEngagement(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating `engagement_score` from TypeForm."""

    __slots__ = []

    def setL1(self):
        self.setEngagementScore()
        print("Set L1")
        self.l1 = L1IndicatorGetter(self)
        print("Defined self.l1")

    def setL2(self):
        self.setEngagementScore()
        self.l2_overlay = L2OverlayWeather(self)
        self.df = self.l2_overlay.set()


class MetricTFEngagement(MetricTFFormatEngagement):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for calculating `engagement_score` from TypeForm."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoEngagement(MetricTFFormatEngagement):
    """Subclass of MetricTFDemo (and thus also MetricFromParameters)
    for calculating `engagement_score` from TypeForm-based demo data."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFPeopleEngaged(MetricTF):
    __slots__ = []

    def setL1(self):
        self.setPeopleEngaged()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setPeopleEngaged()


class MetricTFEngagementMoM(MetricTFEngagement):
    """Subclass of MetricTFEngagement for calculating
    month-over-month `engagement_score` L1s."""

    __slots__ = []

    def setL1(self):
        self.setEngagementScoreMonthOverMonth()
        self.l1 = L1IndicatorGetter(self)


class MetricTFFormatCategoryListCount(MetricTFFormat):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that get pulled as lists from TypeForm and get displayed as counts"""

    __slots__ = []

    def setL1(self):
        self.setCategoryListCount()
        boolmap = {True: "True", False: "False"}
        col = self.params.metric
        df = self.df.replace({col: boolmap})
        # total=df['size'][0]
        df = df[(df[col] != "Blank") & (df[col] != "Unanswered")]
        # df = df[df['count'] == df['count'].max()]
        # df['show'] = round(df['count']*100.0/total, 0).astype('int64')
        # df[col] = df[col].astype(str) + ' ' + df['show'].astype(str) + '%'
        # df.drop(['show'], axis=1, inplace=True)
        self.df = df
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setCategoryListCount()


class MetricTFCategoryListCount(MetricTFFormatCategoryListCount):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that get pulled as lists from TypeForm and get displayed as counts."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoCategoryListCount(MetricTFFormatCategoryListCount):
    """Subclass of MetricTFDemo (and thus also MetricFromParameters) for metrics
    that get pulled as lists from TypeForm-based demo data and
    get displayed as counts."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


# class MetricTFFormatBigFootCategoryCount(MetricTFFormat):
#     """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
#     that can be pulled directly from TypeForm and get displayed as counts."""

#     __slots__ = []

#     def setL1(self):
#         self.setCategoryCount()
#         boolmap = {True: "True", False: "False"}
#         col = self.params.metric
#         df = self.df.replace({col: boolmap})
#         # total=df['size'][0]
#         df = df[(df[col] != "Blank") & (df[col] != "Unanswered")]

#         def condenseTime(row):
#             # row[col] = row[col].replace("–", "-")
#             # row[col] = row[col].replace("Evening - 18.00-00.00", "18h-00h")
#             # row[col] = row[col].split(" - ")
#             # # if len(row[col]) > 1:
#             # #     row[col] = row[col][-1:]
#             # row[col] = " - ".join(row[col])
#             # row[col] = row[col].replace(".00", "h")
#             # row[col] = row[col].replace(" - ", "-")
#             replacements = {".00": "", ":00": "", "-": "h-", "|": "h|"}
#             for k, v in replacements.items():
#                 row[col] = row[col].replace(k, v)
#             return row

#         df = df.apply(condenseTime, axis=1)
#         self.df = df
#         self.l1 = L1IndicatorGetter(self)

#     def setL2(self):
#         self.setCategoryCount()


# class MetricTFBigFootCategoryCount(MetricTFFormatBigFootCategoryCount):
#     """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
#     that get pulled as lists from TypeForm and get displayed as counts."""

#     __slots__ = []

#     def setTableName(self):
#         self.table_name = "tf_queries"


# class MetricTFDemoBigFootCategoryCount(MetricTFFormatBigFootCategoryCount):
#     """Subclass of MetricTFDemo (and thus also MetricFromParameters) for metrics
#     that get pulled as lists from TypeForm-based demo data and
#     get displayed as counts."""

#     __slots__ = []

#     def setTableName(self):
#         self.table_name = "tf_demo_queries"


class MetricTFFormatAgeRanges(MetricTFFormatCategoryListCount):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that get pulled as lists from TypeForm and get displayed as counts"""

    __slots__ = []

    def setL1(self):
        self.setCategoryListCount()
        boolmap = {True: "True", False: "False"}
        col = self.params.metric
        df = self.df.replace({col: boolmap})
        df = df[(df[col] != "Blank") & (df[col] != "Unanswered")]
        for i in [" years", " Years", " ans", " Jahre"]:
            df[col] = df[col].str.replace(i, "")  # Drop for l1 to save space.
        for i in ["Under", "Unter"]:
            df[col] = df[col].str.replace(i, "<")
        self.df = df
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setCategoryListCount()


class MetricTFAgeRanges(MetricTFFormatAgeRanges):
    """Subclass of MetricTF (and thus also MetricFromParameters) for metrics
    that get pulled as lists from TypeForm and get displayed as counts."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class MetricTFDemoAgeRanges(MetricTFFormatAgeRanges):
    """Subclass of MetricTFDemo (and thus also MetricFromParameters) for metrics
    that get pulled as lists from TypeForm-based demo data and
    get displayed as counts."""

    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class MetricTFSampleDemographic(MetricTFFormat):
    def setTableName(self):
        self.table_name = "tf_queries"

    def setL1(self):
        self.setSampleDemographic()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setSampleDemographic()


class MetricTFReportedIssuesXfctr(MetricTF):
    """Return reported issues for XFCTR.

    L1: most common category of issues reported,
    L2: pie of issue categories & table of all issues,
    CSV: table of all issues.
    """

    def reported_issues_table_xfctr(self):
        sql = """
        WITH base AS (
            SELECT  response_id, value, field_ref,
                    week,
                    day_of_week,
                    location
            FROM tf_queries m
            WHERE TRUNC(date) >= '{startDate}'
            AND TRUNC(date) <= '{endDate}'
            AND field_ref IN ('cart_issue_options', 'cart_issue_other',
                            'inventory_issue_options', 'inventory_issue_other', 
                            'event_checklist_issue_options', 'event_checklist_issue_other'
                            'store_issue_options', 'store_issue_other',
                            'circle_issue_options', 'circle_issue_other'){filter_wc}{job_filter}
        ), arr AS (
            SELECT  SPLIT_TO_ARRAY(TRIM('[]' FROM REPLACE(value, '"', '')), ',') AS arr,
                    field_ref,
                    week,
                    day_of_week,
                    location
            FROM base
            WHERE field_ref LIKE '%%_options'
        ), options AS (
            SELECT  TRIM(BOTH '"' FROM value::VARCHAR) AS value,
                    field_ref,
                    week,
                    day_of_week,
                    location
            FROM arr AS a
            JOIN a.arr AS value ON TRUE
        ), other AS (
            SELECT  value,
                    field_ref,
                    week,
                    day_of_week,
                    location
            FROM base
            WHERE field_ref LIKE '%%_other'
        ), grouped AS (
            SELECT  value, COUNT(value) AS count, field_ref,
                    week,
                    day_of_week,
                    location
            FROM options
            WHERE value != 'Other'
            GROUP BY value, field_ref,
                    week,
                    day_of_week,
                    location
            UNION ALL
            SELECT  value, COUNT(value) AS count, field_ref,
                    week,
                    day_of_week,
                    location
            FROM other
            GROUP BY value, field_ref,
                    week,
                    day_of_week,
                    location
        ), size AS (
            SELECT COUNT(DISTINCT response_id) AS size
            FROM base
        )
        SELECT  field_ref, value AS {col}, count, s.size, 
                week,
                day_of_week,
                location
        FROM grouped g
        JOIN size s ON TRUE
        ORDER BY count DESC, field_ref, {col}
        ;"""
        sql = self.fill_sql_inserts(sql)
        return self.make_df(sql, to_self=False)

    def reported_issues_default_xfctr(self):
        l1_limit = ""
        if self.params.layer == "1":
            l1_limit = "LIMIT 1"
        sql = """
        WITH base AS (
            SELECT  value, field_ref{full_select_insert}
            FROM tf_queries m
            WHERE TRUNC(date) >= '{startDate}'
            AND TRUNC(date) <= '{endDate}'
            AND field_ref IN ('cart_issue_yn', 'inventory_issue_yn', 'event_checklist_issue_yn', 'store_issue_yn', 'circle_issue_yn')
            AND value = 'True'{filter_wc}{job_filter}
        ), grouped AS (
            SELECT COUNT(value) AS count, INITCAP(REPLACE(REPLACE(field_ref, '_issue_yn', ''), '_', ' ')) AS {col}{by}
            FROM base
            GROUP BY field_ref{by}
            ORDER BY count DESC, {col}{by}
            {l1_limit}
        ), size AS (
            SELECT COUNT(*) AS size
            FROM base
        )
        SELECT {col}, count, s.size{by}
        FROM grouped
        JOIN size s ON TRUE
        ORDER BY count DESC, {col}{by}
        ;"""
        sql = self.fill_sql_inserts(sql, {"l1_limit": l1_limit})
        return self.make_df(sql)

    #     Cart Issues
    #     Inventory Issues
    #     Event Checklist issues
    #     Store Issues
    #     Circle Offer Issues

    # cart_issue_yn_dlw
    # cart_issue_options_dlw
    # cart_issue_other_dlw
    # inventory_issue_yn_dlw
    # inventory_issue_options_dlw
    # inventory_issue_other_dlw
    # event_checklist_issue_yn_dlw
    # event_checklist_issue_options_dlw
    # event_checklist_issue_other_dlw
    # store_issue_yn_dlw
    # store_issue_options_dlw
    # store_issue_other_dlw
    # circle_issue_yn_dlw
    # circle_issue_options_dlw
    # circle_issue_other_dlw

    def setL1(self):
        self.reported_issues_default_xfctr()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.reported_issues_default_xfctr()

    def set_return_dict(self):
        table = self.reported_issues_table_xfctr()
        self.return_dict = {
            "plot": self.df.to_dict(orient="records"),
            "dataTable": table.to_dict(orient="records"),
        }

    def setFullExport(self):
        table = self.reported_issues_table_xfctr()
        self.return_dict = table.to_csv(index=False)
        self.useS3("csv", use_json=True)


class MetricTFHighlightedProducts(MetricTF):
    def setL1(self):
        self.set_highlighted_products()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_highlighted_products()

    def set_return_dict(self):
        df = self.df
        names = {k: v for k, v in zip(df["stack"], df["value"])}
        names["grouping"] = self.params.sby
        if self.params.sby == "":
            df["ndx"] = ""
            names["grouping"] = "ndx"
        plot = df.pivot(index=names["grouping"], columns="stack", values="cnt")
        plot = plot.fillna(0).astype(int).reset_index()
        labels = df.pivot(index=names["grouping"], columns="stack", values="label")
        labels = labels.reset_index()
        merged_df = plot.merge(
            labels,
            on=names["grouping"],
            how="inner",
            suffixes=("", "_label"),
        )
        print(merged_df)
        self.return_dict = {
            "plot": merged_df.to_dict(orient="records"),
            "stack_names": names,
        }

    def setFullExport(self):
        sql = """
        WITH base AS (
            SELECT  form_id, 
                    response_id,
                    field_ref,
                    value,
                    product_type::VARCHAR, 
                    location,
                    activity_type::VARCHAR,
                    date,
                    day_of_week,
                    week,
                    month,
                    year
            FROM {table_name} m
            LEFT JOIN m.pt AS product_type ON TRUE
            LEFT JOIN m.act AS activity_type ON TRUE
            WHERE TRUNC(m.date) >= '{startDate}'
            AND TRUNC(m.date) <= '{endDate}'
            and m.field_ref LIKE '{col}%%'{filter_wc}{job_filter}
        ), arr AS (
            SELECT  SPLIT_TO_ARRAY(TRIM('[]' FROM REPLACE(value, '"', '')), ',') AS arr,
                    field_ref,
                    response_id
            FROM base
        ), exploded AS (
            SELECT  TRIM(BOTH '"' FROM value::VARCHAR) AS value,
                    field_ref,
                    response_id 
            FROM arr AS a
            JOIN a.arr AS value ON TRUE
        )
        SELECT  b.form_id, 
                b.response_id,
                b.field_ref,
                e.value,
                b.product_type, 
                b.location,
                b.activity_type,
                b.date,
                b.day_of_week,
                b.week,
                b.month,
                b.year
        FROM exploded e
        JOIN base b ON e.response_id = b.response_id AND e.field_ref = b.field_ref
        ORDER BY date, response_id, field_ref
        ;"""
        sql = sql.format(
            col=self.params.metric,
            startDate=self.params.startDate,
            endDate=self.params.endDate,
            filter_wc=self.params.filters_string,
            job_filter=self.params.job_filter,
            table_name=self.table_name,
        )
        df = self.make_df(sql, to_self=False)
        self.return_dict = df.to_csv(index=False)
        self.useS3("csv", use_json=True)


class MetricExecDash(MetricWithIndicators):
    __slots__ = []

    def setAllDates24Months(self, table_name):
        sql = f"""
        SELECT *
        FROM base_{table_name}_all_dates
        ORDER BY date
        ;"""
        dts = pd.read_sql(sql, self.engine)
        dts = list(dts["date"])
        dr = pd.date_range(dts[0], dts[-1])
        da = dr.isin(dts)  # date array
        da = [int(i) for i in da]
        self.startDate = str(dts[0])
        self.endDate = str(dts[-1])
        self.startAt = (
            x_months_ago(24) if x_months_ago(24) > self.startDate else self.startDate
        )
        self.endAt = self.endDate
        self.dateArray = da

    def setL1(self):
        self.setMetric()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setMetric()
        self.df = getValueForEveryGrouping(
            self.df, self.params.metric, self.params.by, target="", dindex="month"
        )

    def setFullExport(self):
        self.setMetric()
        self.useS3("csv", use_json=True)


class MetricLostDaysStyle(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDates24Months("lost")

    def setMetric(self, rnk_limit=8, special_metric=None):
        sql = f"""
        WITH base AS (
            SELECT * 
            FROM base_lost
            WHERE month IN ({self.make_months_list()}){self.params.filters_string}
        )
        """
        if self.params.layer == "csv":
            sql = sql + "SELECT * FROM base ORDER BY month, role_id;"
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            return
        metric = self.params.metric
        ui = self.ui
        if special_metric is not None:
            metric = special_metric
        if (self.params.by != "") and (self.params.layer != "1"):
            by = self.params.ai["by"]
            h3 = self.params.by_dict["hierarchy3"]["select"]
            h4 = self.params.by_dict["hierarchy4"]["select"]
            h5 = self.params.by_dict["hierarchy5"]["select"]
            sql = (
                self.create_month_index()
                + sql
                + f""", pre_rank AS (
            SELECT  number_of_staff AS shifts,
                    {metric},
                    role_id,
                    month,
                    paid_unapproved{h3}{h4}{h5}
            FROM base
        ), grouped AS (
            SELECT  month,
                    {by},
                    SUM({metric}) AS {metric},
                    SUM(shifts) AS shifts
            FROM pre_rank
            GROUP BY month, {by}        
        ), rank AS (
            SELECT  {by}, 
                    DENSE_RANK () OVER (ORDER BY SUM({metric}) DESC, {by}) AS sub_rnk
            FROM pre_rank
            GROUP BY {by}
        ), joined AS (
            SELECT  tis.date_str AS month,
                    COALESCE({metric}, 0) AS {metric}_ref, 
                    COALESCE(shifts, 0) AS shifts, 
                    CASE WHEN sub_rnk <= {rnk_limit} 
                         THEN sub_rnk ELSE {rnk_limit+1} END AS rnk,
                    CASE WHEN rnk <= {rnk_limit} 
                         THEN r.{by}::VARCHAR ELSE 'Other' END AS {by}
            FROM date_index_{ui} tis
            LEFT JOIN grouped g ON tis.date_str = g.month
            LEFT JOIN rank r ON g.{by} = r.{by}
        )
        SELECT  month,
                SUM({metric}_ref) AS {metric},
                SUM(shifts) AS size,
                CASE WHEN size > 0 THEN ROUND({metric}*1.00/size, 2) ELSE 0 END AS frac,
                {by}, 
                rnk
        FROM joined
        GROUP BY month, {by}, rnk
        ORDER BY month, {by}, rnk;
        """
            )
        else:
            sql = (
                self.create_month_index()
                + sql
                + f""", grouped AS (
            SELECT  month,
                    SUM({metric}) AS {metric},
                    SUM(number_of_staff) AS shifts
            FROM base
            GROUP BY month
        ), joined AS (
            SELECT  tis.date_str AS month,
                    COALESCE({metric}, 0) AS {metric}_ref, 
                    COALESCE(shifts, 0) AS shifts
            FROM date_index_{ui} tis
            LEFT JOIN grouped g ON tis.date_str = g.month
        )
        SELECT  month,
                SUM({metric}_ref) AS {metric},
                SUM(shifts) AS size, 
                CASE WHEN size > 0 THEN ROUND({metric}*1.00/size, 2) ELSE 0 END AS frac,
                1 AS rnk
        FROM joined
        GROUP BY month
        ORDER BY month;
        """
            )
        self.make_df(sql)


class MetricCRMActive(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDates24Months("active")

    def setMetric(self, rnk_limit=8):
        if self.params.layer == "csv":
            # Return fuller data; base_active is whittled down to month-level summaries.
            sql = """
            SELECT * 
            FROM base_active_l2
            WHERE month <= {endDate}
              AND month >= {startDate}{filter_wc}
            ;"""
            sql = self.fill_sql_inserts(sql)
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            return
        if self.params.layer == "1":
            sql = "SELECT * FROM base_active;"
        elif self.params.layer == "2":
            sql = f"""
        WITH base AS (
            SELECT * FROM base_active_l2
            WHERE month IN ({self.make_months_list()}){self.params.filters_string}
        )"""
            if self.params.by == "":
                sql = (
                    sql
                    + """
        SELECT  month,
                SUM(crm_active) AS crm_active,
                1 AS rnk
        FROM base
        GROUP BY month
        """
                )
            else:
                by = self.params.sby
                sql = (
                    sql
                    + f""", ranked AS (
            SELECT  {by}, 
                    DENSE_RANK () OVER (ORDER BY SUM(crm_active) DESC, {by}) AS sub_rnk
            FROM base
            GROUP BY {by}
        )
        SELECT  month, 
                SUM(crm_active) AS crm_active,
                sub_rnk AS rnk,
                r.{by}
        FROM base b
        LEFT JOIN ranked r ON b.{by} = r.{by}
        GROUP BY month, sub_rnk, r.{by}
        """
                )
        self.make_df(sql)


class MetricShiftsOpen(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDates24Months("shifts_open")

    def setMetric(self, rnk_limit=8):
        sql = f"""
        WITH base AS (
            SELECT * 
            FROM base_shifts_open
            WHERE month IN ({self.make_months_list(
                include_start=True)}){self.params.filters_string}
        )
        """
        if self.params.layer == "csv":
            sql = sql + "SELECT * FROM base ORDER BY month, region;"
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            return
        metric = self.params.metric
        ui = self.ui
        by = self.params.sby
        if (self.params.by != "") and (self.params.layer != "1"):
            sql = (
                self.create_month_index(True)
                + sql
                + f""", rank AS (
            SELECT  {by}, 
                    DENSE_RANK () OVER (ORDER BY SUM({metric}) DESC, {by}) AS sub_rnk
            FROM base
            GROUP BY {by}
        ), joined AS (
            SELECT  tis.date_str AS month,
                    COALESCE(shifts_required, 0) AS shifts_required, 
                    COALESCE({metric}, 0) AS {metric}_ref, 
                    CASE WHEN sub_rnk <= {rnk_limit} 
                         THEN sub_rnk ELSE {rnk_limit+1} END AS rnk,
                    CASE WHEN rnk <= {rnk_limit} 
                         THEN r.{by}::VARCHAR ELSE 'Other' END AS {by}
            FROM date_index_{ui} tis
            LEFT JOIN base b ON tis.date_str = b.month
            LEFT JOIN rank r ON b.{by} = r.{by}
        )
        SELECT  month,
                SUM({metric}_ref) AS {metric},
                SUM(shifts_required) AS size,
                CASE WHEN size > 0 THEN ROUND({metric}*1.00/size, 2) ELSE 0 END AS frac,
                {by},
                rnk
        FROM joined
        GROUP BY month, {by}, rnk
        ORDER BY month, {by}, rnk;
        """
            )
        else:
            sql = (
                self.create_month_index(True)
                + sql
                + f""", joined AS (
            SELECT  tis.date_str AS month,
                    COALESCE({metric}, 0) AS {metric}_ref, 
                    COALESCE(shifts_required, 0) AS shifts_required
            FROM date_index_{ui} tis
            LEFT JOIN base b ON tis.date_str = b.month
        )
        SELECT  month,
                SUM({metric}_ref) AS {metric},
                SUM(shifts_required) AS size, 
                CASE WHEN size > 0 THEN ROUND({metric}*1.00/size, 2) ELSE 0 END AS frac,
                1 AS rnk
        FROM joined
        GROUP BY month
        ORDER BY month;
        """
            )
        self.make_df(sql)


class MetricEnabledStaffStyle(MetricExecDash):
    @abstractmethod
    def setCSVTable(self):
        pass

    @abstractmethod
    def setBaseTable(self):
        pass

    def setMetric(self, rnk_limit=8):
        if self.params.layer == "csv":
            # Return fuller data; base_active is whittled down to month-level summaries.
            sql = """
            SELECT * 
            FROM {csv_table}
            WHERE date <= '{endDate}'
              AND date >= '{startDate}'{filter_wc}
            ;"""
            sql = self.fill_sql_inserts(sql, {"csv_table": self.setCSVTable()})
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            return
        metric = self.params.metric
        sql = f"""
        WITH base AS (
            SELECT * FROM {self.setBaseTable()}
            WHERE month IN ({self.make_months_list()}){self.params.filters_string}
        )"""
        if self.params.layer == "1":
            sql = (
                sql
                + f"""
        SELECT SUM(staff) AS {metric}
        FROM base
        ;"""
            )
        else:
            ui = self.ui
            sql = self.create_month_index() + sql
            if self.params.by == "":
                sql = (
                    sql
                    + f""", intermediate AS (
            SELECT  SUM(cum_staff) AS cum_staff,
                    month
            FROM base
            GROUP BY month
        )
        SELECT  d.date_str AS month,
                COALESCE(cum_staff, 0) AS {metric},
                1 AS rnk
        FROM date_index_{ui} d
        LEFT JOIN intermediate i ON d.date_str = i.month
        ;"""
                )
            else:
                by = self.params.ai["by"]
                sql = (
                    sql
                    + f""", intermediate AS (
            SELECT  SUM(cum_staff) AS cum_staff,
                    month,
                    {by}
            FROM base
            GROUP BY month, {by}
        ), ranked AS (
            SELECT  {by}, 
                    DENSE_RANK () OVER (ORDER BY SUM(cum_staff) DESC, {by}) AS sub_rnk
            FROM intermediate
            GROUP BY {by}
        )
        SELECT  d.date_str AS month,
                COALESCE(cum_staff, 0) AS {metric}, 
                sub_rnk AS rnk,
                r.{by}::VARCHAR AS {by}
        FROM date_index_{ui} d
        LEFT JOIN intermediate i ON d.date_str = i.month
        LEFT JOIN ranked r ON i.{by} = r.{by}
        ORDER BY month, rnk
        """
                )
        self.make_df(sql)


class MetricEnabledStaff(MetricEnabledStaffStyle):
    __slots__ = []

    def setCSVTable(self):
        return "base_staff"

    def setBaseTable(self):
        return "base_enabled_staff"

    def setAllDates(self):
        self.setAllDates24Months("staff")


class MetricVideoProfiles(MetricEnabledStaffStyle):
    __slots__ = []

    def setCSVTable(self):
        return "base_video"

    def setBaseTable(self):
        return "base_cumulative_video"

    def setAllDates(self):
        self.setAllDates24Months("video")


class MetricNewEnabledStaff(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDates24Months("staff")

    def setMetric(self, rnk_limit=8):
        sql = f"""
        WITH base AS (
            SELECT * 
            FROM base_staff
            WHERE month IN ({self.make_months_list()}){self.params.filters_string}
        )"""
        if self.params.layer == "csv":
            self.make_df(sql + " SELECT * FROM base ORDER BY month;")
            self.return_dict = self.df.to_csv(index=False)
            return
        metric = self.params.metric
        ui = self.ui
        sql = self.create_month_index() + sql
        # if self.params.layer == "1":
        #     sql = sql + f"""
        # SELECT  COUNT(staff_member_id) AS {metric}
        # FROM base
        # """
        # else:
        desc = ""
        if self.params.by == "" or self.params.layer == "1":
            if self.params.layer == "1":
                desc = " DESC"
            sql = (
                sql
                + f""", ready AS (
            SELECT  COUNT(DISTINCT staff_member_id) AS {metric},
                    month, 
                    1 AS rnk
            FROM base
            GROUP BY month
        )
        """
            )
        else:
            by = self.params.ai["by"]
            sql = (
                sql
                + f""", grouped AS (
            SELECT  COUNT(DISTINCT staff_member_id) AS staff,
                    month,
                    {by}
            FROM base
            GROUP BY month, {by}
            ORDER BY month, {by}
        ), ranked AS (
            SELECT  {by}, 
                    DENSE_RANK () OVER (ORDER BY SUM(staff) DESC, {by}) AS sub_rnk
            FROM grouped
            GROUP BY {by}
        ),{self.params.target_cte} ready AS (
            SELECT  d.date_str AS month,
                    COALESCE(staff, 0) AS {metric}, 
                    sub_rnk AS rnk,
                    g.{by}::VARCHAR AS {by}{self.params.target}
            FROM date_index_{ui} d
            LEFT JOIN grouped g ON d.date_str = g.month
            LEFT JOIN ranked r ON g.{by} = r.{by}
            {self.params.target_join}
        )
        """
            )
        sql = (
            sql
            + f"""
        SELECT  *
        FROM ready
        ORDER BY month{desc}, rnk;
        """
        )
        self.make_df(sql)

    def setL2(self):
        self.setMetric()
        self.df = getValueForEveryGrouping(
            self.df, self.params.metric, self.params.by, target=True, dindex="month"
        )


class MetricClientNPS(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDatesSimple("base_client_nps_all_dates")

    def setMetric(self):
        sql = f"""
        WITH base AS (
            SELECT  form_id, 
                    response_id, 
                    client_nps, 
                    date,
                    LEFT(date, 7) AS month,
                    region,
                    client
            FROM base_client_nps m
            WHERE date >= '{self.params.startDate}'
              AND date <= '{self.params.endDate}'{self.params.filters_string}
            ORDER BY form_id, date
        )"""
        if self.params.layer == "csv":
            self.make_df(sql + " SELECT * FROM base ORDER BY month;")
            self.return_dict = self.df.to_csv(index=False)
            return
        desc = ""
        if self.params.layer == "1":
            desc = " DESC"
            by = "month"
            by_select = ", month"
            sql = (
                sql
                + f""", intermediate AS (
            SELECT client_nps, {by}
            FROM base
        )
        """
            )
        elif self.params.by == "":
            by = "grouping_col"
            by_select = ""
            sql = (
                sql
                + f""", intermediate AS (
            SELECT client_nps, 1 AS {by}
            FROM base
        )
        """
            )
        else:  # if (self.params.by != ""):
            by = self.params.ai["by"]
            by_select = self.params.by
            region_select = self.params.by_dict["region"]["select"]
            h6_select = self.params.by_dict["hierarchy6"]["select"]
            h6_join = self.params.by_dict["hierarchy6"]["join"]
            sql = (
                sql
                + f""", intermediate AS (
            SELECT  client_nps{region_select}{h6_select}
            FROM base m{h6_join}
        )
        """
            )
        sql = (
            sql
            + f"""
        SELECT  ROUND(SUM(client_nps) *100.0 /COUNT(client_nps))::INTEGER AS client_nps,
                COUNT(client_nps) AS size{by_select}{self.params.target}
        FROM intermediate
        GROUP BY {by}
        ORDER BY {by}{desc}
        ;"""
        )
        self.make_df(sql)

    def setL2(self):
        self.setMetric()


class MetricPercentNewRecruitment(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDates24Months("pnr")

    def setMetric(self, rnk_limit=8):
        sql = f"""
        WITH base AS (
            SELECT *
            FROM base_pnr
            WHERE month IN ({self.make_months_list()}){self.params.filters_string}
        )
        """
        if self.params.layer == "csv":
            self.make_df(sql + " SELECT * FROM base ORDER BY month;")
            self.return_dict = self.df.to_csv(index=False)
            return
        by_name = sby_name = us_by_name = by_join = h3 = h4 = h5 = order = ""
        by = self.params.by
        region = self.params.sby if self.params.sby != "" else "region"
        if by != "":
            sby_name = valid_hierarchies.get(by[2:], "")
            if sby_name != "":
                by_name = f", {sby_name}"
                us_by_name = f""",
                        us.{sby_name}"""
                by_join = f"""
                AND us.{sby_name} = nh.{sby_name}"""
                order = f"ORDER BY {self.params.sby}"
                h3 = self.params.by_dict["hierarchy3"]["select"]
                h4 = self.params.by_dict["hierarchy4"]["select"]
                h5 = self.params.by_dict["hierarchy5"]["select"]
        sql = (
            sql
            + f""", unique_staff AS (
            SELECT  COUNT(DISTINCT staff_member_id) AS unique_staff,
                    month{by_name}
            FROM base
            GROUP BY month{by_name}
        ), new_hires AS (
            SELECT  COUNT(DISTINCT staff_member_id) AS new_hires,
                    min_month, 
                    month{by_name}
            FROM base
            GROUP BY min_month, month{by_name}
        ), joined AS (
            SELECT  us.month, 
                    unique_staff,
                    COALESCE(new_hires, 0) AS new_hires{us_by_name}
            FROM unique_staff us
            LEFT JOIN new_hires nh 
            ON us.month = nh.min_month
            AND us.month = nh.month{by_join}
        ), cleaned AS (
            SELECT  1 AS grouping_col,
                    new_hires,
                    unique_staff{h3}{h4}{h5}
            FROM joined
        )
        """
        )
        # This is to add a global / without grouping (wog) pie as well.
        if by != "":
            sql = (
                sql
                + f""", unique_staff_wog AS (
            SELECT  COUNT(DISTINCT staff_member_id) AS unique_staff,
                    month
            FROM base
            GROUP BY month
        ), new_hires_wog AS (
            SELECT  COUNT(DISTINCT staff_member_id) AS new_hires,
                    min_month, 
                    month
            FROM base
            GROUP BY min_month, month
        ), joined_wog AS (
            SELECT  us.month, 
                    unique_staff,
                    COALESCE(new_hires, 0) AS new_hires
            FROM unique_staff_wog us
            LEFT JOIN new_hires_wog nh 
            ON us.month = nh.min_month
            AND us.month = nh.month
        ), cleaned_wog AS (
            SELECT  1 AS grouping_col,
                    new_hires,
                    unique_staff,
                    ' Global'::VARCHAR AS {region}
            FROM joined_wog
        )
        """
            )
        if self.params.layer == "1":
            sql = (
                sql
                + """
        SELECT  ROUND(100.00 * COALESCE(SUM(new_hires), 0)/SUM(unique_staff), 2) 
                    AS percent_new_recruitment,
                SUM(unique_staff) AS unique_staff,
                SUM(COALESCE(new_hires, 0)) AS new_hires,
                month
        FROM joined
        GROUP BY month
        ORDER BY month DESC
        ;"""
            )
        else:
            # This is to add a global / without grouping (wog) pie as well.
            if by != "":
                sql = (
                    sql
                    + f"""
        SELECT  'New hires' AS percent_new_recruitment, 
                SUM(new_hires) AS count, 
                SUM(unique_staff) AS size{by}
        FROM cleaned_wog
        GROUP BY grouping_col{by}
        UNION ALL
        SELECT  'Existing staff' AS percent_new_recruitment, 
                SUM(unique_staff - COALESCE(new_hires, 0)) AS count, 
                SUM(unique_staff) AS size{by}
        FROM cleaned_wog
        GROUP BY grouping_col{by}
        UNION ALL
        """
                )
            # Keep this even if we drop the extra global pie.
            sql = (
                sql
                + f"""
        SELECT  'New hires' AS percent_new_recruitment, 
                SUM(new_hires) AS count, 
                SUM(unique_staff) AS size{by}
        FROM cleaned
        GROUP BY grouping_col{by}
        UNION ALL
        SELECT  'Existing staff' AS percent_new_recruitment, 
                SUM(unique_staff - COALESCE(new_hires, 0)) AS count, 
                SUM(unique_staff) AS size{by}
        FROM cleaned
        GROUP BY grouping_col{by}
        {order}
        ;"""
            )
        self.make_df(sql)

    def setL2(self):
        self.setMetric()


class MetricCSAT(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDates24Months(self.params.metric)

    def setMetric(self, rnk_limit=8):
        if self.params.layer == "csv":
            sql = f"""
            SELECT * 
            FROM base_csat
            WHERE metric = '{self.params.metric.split('_')[1]}'
              AND month IN ({self.make_months_list()})
            ORDER BY month, region, project, job_number, response_id
            ;"""
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            return
        sql = self.create_month_index()
        if self.params.layer != "1":
            sql = (
                sql
                + """
        WITH base AS (
            SELECT  d.date_str AS month,
                    COALESCE(b.value, 0) AS value,
                    COALESCE(b.size, 0) AS size,
                    COALESCE(b.us, 0) AS us,
                    COALESCE(b.us_size,  0) AS us_size,
                    COALESCE(b.uk,  0) AS uk,
                    COALESCE(b.uk_size, 0) AS uk_size,
                    COALESCE(b.fr, 0) AS fr,
                    COALESCE(b.fr_size, 0) AS fr_size,
                --    COALESCE(b.at, 0) AS at,
                  --  COALESCE(b.at_size, 0) AS at_size,
                    COALESCE(b.de, 0) AS de,
                    COALESCE(b.de_size, 0) AS de_size
            FROM date_index_{ui} d
            LEFT JOIN ( SELECT * 
                        FROM base_{col}) b ON d.date_str = b.month   
        )
        SELECT *{target}
        FROM base
        ORDER BY month;
        """
            )
        elif self.params.layer == "1":
            sql = (
                sql
                + """
        WITH base AS (
            SELECT  d.date_str AS month, 
                    --COALESCE(b.value, 0) AS {col},
                    b.value,
                    COALESCE(b.size, 0) AS size
            FROM date_index_{ui} d
            LEFT JOIN base_{col} b ON d.date_str = b.month
        ), scaled AS (
            SELECT  value * size AS mult,
                    size
            FROM base
        )
        SELECT ROUND(1.0 * SUM(mult) / SUM(size), 2) AS {col}{target}
        FROM scaled;
        """
            )
        sql = self.fill_sql_inserts(sql)
        # need to agg overall / not order by month
        # find out whether avg/sum.
        df = self.make_df(sql)
        if self.params.layer == "2":
            df["month"] = pd.to_datetime(df["month"], errors="coerce")
            df["month"] = pd.to_datetime(df["month"]).apply(
                lambda x: datetime.strftime(x, "%B %Y")
            )
        self.df = df


class MetricSurveyComplianceExec(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDates24Months(self.params.metric)

    def setMetric(self):
        metric = self.params.metric
        sql = f"""
        WITH base AS (
            SELECT * 
            FROM base_{metric}
            WHERE month IN ({self.make_months_list()})
        )
        """
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base ORDER BY month;")
            self.return_dict = df.to_csv(index=False)
            return
        if self.params.layer != "1":
            sql = (
                self.create_month_index()
                + sql
                + f""", dated AS (
            SELECT  d.date_str AS month,
                    COALESCE(b.value, 0) AS value,
                    COALESCE(b.size, 0) AS size,
                    COALESCE(b.submitted, 0) AS submitted,
                    COALESCE(b.us, 0) AS us,
                    COALESCE(b.us_size,  0) AS us_size,
                    COALESCE(b.us_submitted, 0) AS us_submitted,
                    COALESCE(b.uk,  0) AS uk,
                    COALESCE(b.uk_size, 0) AS uk_size,
                    COALESCE(b.uk_submitted, 0) AS uk_submitted,
            --        COALESCE(b.at, 0) AS at,
              --      COALESCE(b.at_size, 0) AS at_size,
                --    COALESCE(b.at_submitted, 0) AS at_submitted,
                    COALESCE(b.de, 0) AS de,
                    COALESCE(b.de_size, 0) AS de_size,
                    COALESCE(b.de_submitted, 0) AS de_submitted,
                    COALESCE(b.fr, 0) AS fr,
                    COALESCE(b.fr_size, 0) AS fr_size,
                    COALESCE(b.fr_submitted, 0) AS fr_submitted
            FROM date_index_{self.ui} d
            LEFT JOIN base b ON d.date_str = b.month
        )
        SELECT *, ROUND(size * {self.target} * 1.0 / 100, 1) AS target
        FROM dated
        ORDER BY month;
        """
            )
        elif self.params.layer == "1":
            sql = (
                sql
                + f"""--, spacer AS (
        SELECT  SUM(value) AS sent,
                ROUND(SUM(value) * 100.0 / SUM(size), 1) AS {metric},
                SUM(size) AS size,
                {self.target} AS target
        FROM base
        """
            )
        df = self.make_df(sql)
        if self.params.layer == "2":
            df["month"] = pd.to_datetime(df["month"], errors="coerce")
            df["month"] = pd.to_datetime(df["month"]).apply(
                lambda x: datetime.strftime(x, "%B %Y")
            )
        self.df = df


class MetricVideoOverHires(MetricExecDash):
    __slots__ = []

    def setAllDates(self):
        self.setAllDates24Months("video")

    def setMetric(self):
        col = self.params.metric
        sql = f"""
        WITH base AS (
            SELECT * 
            FROM base_{col}
            WHERE month IN ({self.make_months_list()})
        )"""
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base ORDER BY month;")
            self.return_dict = df.to_csv(index=False)
            return
        if self.params.layer != "1":
            sql = (
                self.create_month_index()
                + sql
                + f"""
        SELECT *, {self.target} AS target
        FROM base
        ORDER BY month;
        """
            )
        elif self.params.layer == "1":
            sql = (
                sql
                + f"""--, spacer AS (
        SELECT  SUM(videos) AS video,
                SUM(hires) AS hires,
                ROUND(SUM(videos) * 100.0 / SUM(hires), 1) AS {col},
                {self.target} AS target
        FROM base
        """
            )
        df = self.make_df(sql)
        if self.params.layer == "2":
            df["month"] = pd.to_datetime(df["month"], errors="coerce")
            df["month"] = pd.to_datetime(df["month"]).apply(
                lambda x: datetime.strftime(x, "%B %Y")
            )
        self.df = df


class MetricZendeskExport(MetricFromParameters):
    def setAllFreelancerUserDates(self):
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
        UNION ALL
        SELECT MIN(updated_at)::DATE AS date FROM tbl
        ORDER BY date
        ;"""
        sql = makeMultiDBQuery(recency_query, start_sql, "            UNION", end_sql)
        dts = pd.read_sql(sql, self.engine)
        self.standardDatesProcessing(dts)

    def setAllDates(self):
        self.setAllFreelancerUserDates()

    def setMetric(self):
        if self.params.layer in ["1"]:
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
        SELECT 'D' || DATE_PART('doy', MAX(updated_at))::VARCHAR || ' ' || 
                MAX(updated_at)::TIME::VARCHAR AS zendesk_export
        FROM tbl
        ;"""
            sql = makeMultiDBQuery(recency_query, start_sql, "        UNION", end_sql)
        if self.params.layer in ["2"]:
            self.df = pd.DataFrame()  # Make same as csv if we add table-view.
            return
        if self.params.layer in ["csv"]:
            query = """
        SELECT  f.firstname || ' ' || f.lastname AS name,
                u.email,
                f.id || '{suffix}' AS external_id,
                NULL AS details,
                NULL AS notes,
                CASE WHEN LEFT(f.mobile, 1) = '+' 
                     THEN REGEXP_REPLACE(f.mobile,'\\\s','') 
                     ELSE COALESCE(f.mobile_dial_code, '') || 
                            REGEXP_REPLACE(f.mobile,'\\\s','') END AS phone,
                NULL AS shared_phone_number,
                'End-user' AS role,
                NULL AS restriction,
                'Global Field Staff (External)' AS organization,
                'user_updated_via_1CRM_extract' AS tags,
                NULL AS brand,
                '1CRM Field Staff Member' AS "custom_fields.8006983056017",
                '{country_name}' AS "custom_fields.8007053551121",
                'Field Staff Member' AS "custom_fields.8005867165073",
                CASE WHEN u.deactivated_at IS NOT NULL THEN 'Deactivated'
                        WHEN u.disabled_at    IS NOT NULL THEN 'Locked'
                        WHEN ((u.is_confirmed = 0) AND (f.is_approved = 0)) 
                            THEN 'Not confirmed'
                        WHEN ((u.is_confirmed = 0) AND (f.is_approved = 1)) 
                            THEN 'On-boarding'
                        WHEN ((u.is_confirmed = 1) AND (f.is_approved = 0)) 
                            THEN 'Pending Approval'
                        WHEN ((u.is_confirmed = 1) AND (f.is_approved = 1)) 
                            THEN 'Approved'
                        ELSE 'Unaccounted-for combination' 
                END AS "custom_fields.8005867165073"
        FROM freelancers{suffix} f
        JOIN users{suffix} u ON f.user_id = u.id            
        """
            sql = makeMultiDBQuery(query)
        self.make_df(sql)

    def setL1(self):
        self.setMetric()
        self.l1 = L1DateGetterRecords(self)

    def setL2(self):
        self.setMetric()
        self.return_dict = self.df.to_dict(orient="records")

    def setFullExport(self):
        self.setMetric()
        self.return_dict = self.df.to_csv(index=False)
        self.useS3("csv", use_json=True)


class MetricComplianceDash(MetricWithIndicators):
    __slots__ = []

    def setAllDates(self):
        self.setAllDatesCD(self.params.metric)

    # Probably need to change this.
    def setAllDatesCD(self, table_name):
        based_name = f"base_{table_name}_all_dates"
        self.setAllDatesSimple(based_name)

    def setL1(self):
        self.setMetric()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setMetric()

    def setFullExport(self):
        self.setMetric()
        self.useS3("csv", use_json=True)


class MetricLevelSplit(MetricComplianceDash):
    def setAllDates(self):
        self.setAllDatesCD(self.params.metric)

    def setMetric(self):
        core = """
        WITH base AS (
            SELECT *, LEFT(DATE_TRUNC('month', date)::VARCHAR, 7) AS month
            FROM base_level_split
            WHERE date::DATE >= '{startDate}'
            AND date::DATE <= '{endDate}'{filter_wc}
            ORDER BY date
        )"""
        core = self.fill_sql_inserts(core)
        ui = self.ui
        if self.params.layer == "csv":
            df = self.make_df(core + " SELECT * FROM base ORDER BY date;")
            self.return_dict = df.to_csv(index=False)
            return
        elif self.params.layer == "1":
            sql = (
                self.create_month_index()
                + core
                + f"""
        SELECT  SUM(COALESCE(bronze, 0)) AS bronzes,
                COUNT(project_id) AS dashes,
                CASE WHEN dashes = 0 THEN 0 ELSE 
                     ROUND(100 - bronzes * 100.0 / dashes, 1) END AS level_split,
                date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN base b ON d.date_str = b.month
        GROUP BY date_str
        ORDER BY date_str
        LIMIT 3;"""
            )
        elif self.params.layer == "2":
            sql = (
                self.create_month_index()
                + core
                + f"""
        SELECT  date_str AS month, 
                COALESCE(SUM(bronze), 0) AS level_split,
                COALESCE(SUM(bronze), 0) + COALESCE(SUM(silver), 0) + 
                    COALESCE(SUM(gold), 0) AS size,
                'bronze' AS level,
                1 AS rnk
        FROM date_index_{ui} d
        LEFT JOIN base b ON b.month = d.date_str
        GROUP BY date_str
        UNION
        SELECT  date_str AS month, 
                COALESCE(SUM(silver), 0) AS level_split,
                COALESCE(SUM(bronze), 0) + COALESCE(SUM(silver), 0) + 
                    COALESCE(SUM(gold), 0) AS size,
                'silver' AS level,
                2 AS rnk
        FROM date_index_{ui} d
        LEFT JOIN base b ON b.month = d.date_str
        GROUP BY date_str
        UNION
        SELECT  date_str AS month, 
                COALESCE(SUM(gold), 0) AS level_split,
                COALESCE(SUM(bronze), 0) + COALESCE(SUM(silver), 0) + 
                    COALESCE(SUM(gold), 0) AS size,
                'gold' AS level,
                3 AS rnk
        FROM date_index_{ui} d
        LEFT JOIN base b ON b.month = d.date_str
        GROUP BY date_str
        ORDER BY month, rnk
        ;"""
            )
        self.make_df(sql)


class MetricCorrectionRate(MetricComplianceDash):
    def setAllDates(self):
        self.setAllDatesCD(self.params.metric)

    def setMetric(self):
        core = """SELECT  m.dash_name,
                            m.date,
                            m.form_id,
                            m.submitted_at,
                            m.response_id,
                            m.altered{full_select_insert}
                    FROM (  SELECT  dash_name,
                                    date,
                                    form_id,
                                    submitted_at,
                                    response_id,
                                    altered,
                                    activity_type
                            FROM base_correction_rate ) m{h1_join}{h2_join}
                    WHERE date::DATE >= '{startDate}'
                      AND date::DATE <= '{endDate}'{filter_wc}
                    ORDER BY dash_name, date, form_id"""
        core = self.fill_sql_inserts(core)
        if self.params.layer == "csv":
            df = self.make_df(core)
            self.return_dict = df.to_csv(index=False)
            return
        elif self.params.layer == "1":
            sql = (
                self.create_month_index()
                + """
        SELECT  COUNT(response_id) AS responses,
                COALESCE(SUM(alt), 0) AS alterations,
                CASE WHEN responses = 0 THEN 0 
                     ELSE ROUND(100.0 * alterations / responses, 1) 
                     END AS correction_rate,
                date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN ( SELECT  response_id,
                            MAX(altered) AS alt,
                            month
                    FROM ({core})
                    GROUP BY response_id, month) b ON d.date_str = b.month
        GROUP BY date_str
        ORDER BY date_str
        ;"""
            )
        elif self.params.layer == "2":
            sql = """
        SELECT  --dash_name,
                --date::VARCHAR,
                --form_id,
                COUNT(response_id) AS size,
                SUM(alt) AS alterations,
                CASE WHEN size = 0 THEN 0 
                     ELSE ROUND(100.0 * alterations / size, 1) END AS correction_rate
        FROM (  SELECT  response_id,
                        MAX(altered) AS alt
                FROM ({core})
                GROUP BY response_id)
        --GROUP BY dash_name, date, form_id
        --ORDER BY dash_name, date, form_id
        ;"""
            if self.params.by != "":
                sql = """
        SELECT  COUNT(DISTINCT response_id) AS size,
                SUM(alt) AS alterations,
                CASE WHEN size = 0 THEN 0 
                     ELSE ROUND(100.0 * alterations / size, 1) END AS correction_rate, 
                {sby}
        FROM (  SELECT  response_id,
                        MAX(altered) AS alt,
                        {sby}
                FROM ({core})
                GROUP BY response_id, {sby})
        GROUP BY {sby}
        ORDER BY {sby}
        ;"""
        sql = self.fill_sql_inserts(sql, {"core": core})
        self.make_df(sql)


class MetricEOSCount(MetricComplianceDash):
    def setMetric(self):
        core = """
        SELECT  m.response_id,
                m.date{full_select_insert}
        FROM (  SELECT  response_id,
                        date,
                        submitted_at,
                        activity_type
                FROM base_eos_count) m{h1_join}{h2_join}{act_join}
        WHERE date >= '{startDate}'
        AND date <= '{endDate}'{filter_wc}
        ORDER BY date"""
        core = self.fill_sql_inserts(core)
        if self.params.layer == "csv":
            df = self.make_df(core)
            self.return_dict = df.to_csv(index=False)
            return
        elif self.params.layer == "1":
            sql = (
                self.create_month_index()
                + """
        SELECT  COUNT(DISTINCT response_id) AS eos_count, 
                d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN ( SELECT  response_id,
                            MAX(date) AS date, 
                            LEFT(DATE_TRUNC('month', MAX(date))::VARCHAR, 7) AS month
                    FROM (  {core})
                    GROUP BY response_id) b ON d.date_str = b.month
        GROUP BY date_str
        ORDER BY date_str 
        ;"""
            )
        elif self.params.layer == "2":
            sql = """
        SELECT  COUNT(DISTINCT response_id) AS eos_count
        FROM (  SELECT  response_id,
                        MAX(date) AS date
                FROM (  {core})
                GROUP BY response_id)
        ;"""
            if self.params.by != "":
                sql = """
        SELECT  COUNT(DISTINCT response_id) AS eos_count{by}
        FROM (  SELECT  response_id,
                        MAX(date) AS date{by}
                FROM (  {core})
                GROUP BY response_id{by})
        WHERE {sby} IS NOT NULL
        GROUP BY {sby}
        ;"""
        sql = self.fill_sql_inserts(sql, {"core": core})
        self.make_df(sql)


class MetricEOSRate(MetricComplianceDash):
    def setAllDates(self):
        self.setAllDatesCD(self.params.metric)

    def setMetric(self):
        core = """
                SELECT *{year_select}{month_select}{week_select}{dow_select}
                FROM base_eos_rate
                WHERE date >= '{startDate}'
                AND date <= '{endDate}'{filter_wc}
                ORDER BY date"""
        core = self.fill_sql_inserts(core)
        if self.params.layer == "csv":
            df = self.make_df(core)
            self.return_dict = df.to_csv(index=False)
            return
        elif self.params.layer == "1":
            sql = (
                self.create_month_index()
                + """
        SELECT  SUM(eos_submissions) AS eos_submitted, 
                SUM(event_count) AS events_counted, 
                ROUND(eos_submitted * 100.0 / events_counted, 1) AS eos_rate,
                d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN ({core}) b ON d.date_str = b.month
        GROUP BY date_str
        ORDER BY date_str 
        ;"""
            )
        elif self.params.layer == "2":
            sql = """
        SELECT  SUM(eos_submissions) AS eos_submitted, 
                SUM(event_count) AS events_counted, 
                ROUND(eos_submitted * 100.0 / events_counted, 1) AS eos_rate
        FROM (  {core})
        ;"""
            if self.params.by != "":
                sql = """
        SELECT  SUM(eos_submissions) AS eos_submitted, 
                SUM(event_count) AS events_counted, 
                ROUND(eos_submitted * 100.0 / events_counted, 1) AS eos_rate{by}
        FROM (  {core})
        GROUP BY {sby}
        ;"""
        sql = self.fill_sql_inserts(sql, {"core": core})
        self.make_df(sql)


# From here to line ~3175 will need cleaning up when sustainability-dash is complete
class MetricInSustainabilityDash(MetricWTableName):
    __slots__ = []

    def setFullExport(self):
        sql = """
        SELECT  *
        FROM {table_name}
        WHERE date >= '{startDate}'
          AND date <= '{endDate}'{filter_wc}
        ORDER BY date
        ;"""
        sql = self.fill_sql_inserts(sql)
        df = self.make_df(sql, to_self=False)
        self.return_dict = df.to_csv(index=False)

    def setAllDates(self):
        self.setAllDatesSimple(self.table_name)

    def generic_stack_handle_month(self):
        table_name = self.table_name
        base_sql = f"""
        WITH base AS (
            SELECT * 
            FROM {table_name}
            WHERE month IN ({self.make_months_list()}){self.params.filters_string}
        )
        """
        return base_sql, self.create_month_index

    def generic_stack_handle_trees_planted_quarter(self):
        table_name = self.table_name
        base_sql = f"""
        WITH base AS (
            SELECT date, year, quarter AS month, client, trees_planted, carbon_offset 
            FROM {table_name}
            WHERE month IN ({self.make_quarters_list()}){self.params.filters_string}
        )
        """
        return base_sql, self.create_quarter_index

    def set_generic_stacked_by_month(
        self, month_func=None, rnk_limit=8, special_metric=None
    ):
        """Like LostDaysStyle's .setMetric() but generic."""
        if month_func is None:
            base_sql, create_index = self.generic_stack_handle_month()
        else:
            base_sql, create_index = month_func()
        metric = self.params.metric
        if special_metric is not None:
            metric = special_metric
        ui = self.ui
        if self.params.layer == "csv":
            sql = base_sql + "SELECT * FROM base ORDER BY month, role_id;"
            df = self.make_df(sql)
            self.return_dict = df.to_csv(index=False)
            return
        if (self.params.by != "") and (self.params.layer != "1"):
            by = self.params.ai["by"]
            sql = (
                create_index()
                + base_sql
                + f""", pre_rank AS (
            SELECT  {metric},
                    month,
                    {by}
            FROM base
        ), grouped AS (
            SELECT  month,
                    {by},
                    SUM({metric}) AS {metric},
                    COUNT({metric}) AS size
            FROM pre_rank
            GROUP BY month, {by}        
        ), rank AS (
            SELECT  {by}, 
                    DENSE_RANK () OVER (ORDER BY SUM({metric}) DESC, {by}) AS sub_rnk
            FROM pre_rank
            GROUP BY {by}
        ), distinct_groupings AS (
            SELECT DISTINCT {by}
            FROM rank
        ), all_groupings AS (
            SELECT date_str, {by}
            FROM date_index_{ui}
            CROSS JOIN distinct_groupings 
        ), joined AS (
            SELECT  tis.date_str AS month,
                    COALESCE({metric}, 0) AS {metric}_ref, 
                    COALESCE(size, 0) AS size_ref, 
                    CASE WHEN sub_rnk <= {rnk_limit} 
                         THEN sub_rnk ELSE {rnk_limit+1} END AS rnk,
                    CASE WHEN rnk <= {rnk_limit} 
                         THEN r.{by}::VARCHAR ELSE 'Other' END AS {by}
            FROM all_groupings tis
            LEFT JOIN grouped g ON tis.date_str = g.month AND tis.{by} = g.{by}
            LEFT JOIN rank r ON tis.{by} = r.{by}
        )
        SELECT  month,
                ROUND(SUM({metric}_ref), 2) AS {self.params.metric},
                SUM(size_ref) AS size,
                {by}, 
                rnk
        FROM joined
        GROUP BY month, rnk, {by}
        ORDER BY month, rnk, {by};
        """
            )
        else:
            sql = (
                create_index()
                + base_sql
                + f""", grouped AS (
            SELECT  month,
                    SUM({metric}) AS {metric},
                    COUNT({metric}) AS size
            FROM base
            GROUP BY month
        ), joined AS (
            SELECT  tis.date_str AS month,
                    COALESCE({metric}, 0) AS {metric}_ref, 
                    COALESCE(size, 0) AS size_ref
            FROM date_index_{ui} tis
            LEFT JOIN grouped g ON tis.date_str = g.month
        )
        SELECT  month,
                ROUND(SUM({metric}_ref), 2) AS {self.params.metric},
                SUM(size_ref) AS size,
                1 AS rnk
        FROM joined
        GROUP BY month
        ORDER BY month;
        """
            )
        self.make_df(sql)

    def set_mode_of_transport(self):
        sql = """
        SELECT {col}, count, size{by}
        FROM  ( SELECT 
                {col}, 
                Count(distinct(contact)) as size, 
                COUNT(contact) AS count{by}
                FROM {table_name}
                WHERE date >= '{startDate}'
                AND date <= '{endDate}'{filter_wc}{job_filter}
                GROUP BY 1{by})
        GROUP BY {col},count,size{by}
        ORDER BY count{by}
        ;"""
        if self.params.sby == "":
            sdotby = ""
        else:
            sdotby = f", s.{self.params.sby}"
        sql = self.fill_sql_inserts(sql, {"sdotby": sdotby})
        self.make_df(sql)

    def set_cfs_score(self):
        # Could descend from a more general method.
        sql = """
        WITH base AS (
            SELECT 1 AS grouping_col, *
            FROM {table_name}
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}{job_filter}
        )
        SELECT  ROUND(SUM(sustainability_score),2) AS {col},
                COUNT(sustainability_score) AS size{by}
        FROM base
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        """
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def set_sus_compliance_rate(self):
        sql = """
        WITH base AS (
            SELECT 1 AS grouping_col, *
            FROM {table_name}
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}{job_filter}
        )
        SELECT  COUNT(CASE WHEN distance_km IS NOT NULL
                            AND mode_of_transport IS NOT NULL THEN 1 END) AS submitted,
                COUNT(DISTINCT(staff_member_id)) AS required,
                ROUND(100.0 * submitted/required, 2) AS {col}{by}
        FROM base
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def set_perma_distance_km(self):
        """setDistance() for union of sus_field_staff_sustainability and TF values."""
        sql = """
        WITH base AS ( 
            SELECT 1 AS grouping_col,*
            FROM sus_field_staff_sustainability m
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}{job_filter}   
        )
        SELECT  SUM({col}::INTEGER * 1.0) AS {col}, COUNT({col}) AS size{by}{target}
        FROM base m     
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def set_perma_mot(self):
        """CategoryCount() for union of tf values and sus_field_staff_sustainability."""
        l1_where_insert = ""
        l1_limit_insert = ""
        if self.params.layer == "1":
            l1_where_insert = (
                f"WHERE {self.params.metric} NOT IN " "('Blank', 'Unanswered')"
            )
            l1_limit_insert = "LIMIT 1"

        sql = """
        WITH base AS (
            SELECT  CASE WHEN {col} = '' THEN 'Blank'
                         WHEN {col} IS NOT NULL THEN {col}
                         ELSE 'Unanswered' END AS {col},
                         1 AS grouping_col{by}
            FROM  {table_name} m
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}{job_filter}        
        ), counted AS (
            SELECT  grouping_col,
                    COUNT({col}) AS count,
                    {col}{by}
            FROM base
            GROUP BY grouping_col, {col}{by}  
        ), size AS (
            SELECT 1 AS dummy, SUM(grouping_col) AS size{by}
            FROM base
            GROUP BY dummy{by}
        )
        SELECT {col}, count, size{sdotby}
        FROM  counted m
        JOIN  size s ON TRUE{byjoin}
        {l1_where_insert}
        ORDER BY count DESC{sdotby}
        {l1_limit_insert}
        ;"""
        # print(sql)
        if self.params.sby == "":
            sdotby = ""
            byjoin = ""
        else:
            sdotby = f", s.{self.params.sby}"
            byjoin = f" AND s.{self.params.sby} = m.{self.params.sby}"
        sql = self.fill_sql_inserts(
            sql,
            {
                "sdotby": sdotby,
                "byjoin": byjoin,
                "l1_where_insert": l1_where_insert,
                "l1_limit_insert": l1_limit_insert,
            },
        )
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)


class MetricFromFieldStaff(MetricInSustainabilityDash):
    """Subclass of MetricTFFormat (and thus also MetricFromParameters)
    for calculating `distance_km` from TypeForm data."""

    __slots__ = []

    def setL1(self):
        self.set_generic_stacked_by_month()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month()

    def setTableName(self):
        self.table_name = "sus_field_staff_sustainability"


class MetricFromFieldStaffKM(MetricInSustainabilityDash):
    """Subclass of MetricTFFormat (and thus also MetricFromParameters)
    for calculating `distance_km` from TypeForm data."""

    __slots__ = []

    def setL1(self):
        self.set_generic_stacked_by_month(special_metric="distance_km")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month(special_metric="distance_km")

    def setTableName(self):
        self.table_name = "sus_field_staff_sustainability"


class MetricFromFieldStaffSustainability(MetricInSustainabilityDash):
    """Subclass of MetricTFFormat (and thus also MetricFromParameters)
    for calculating `distance_km` from TypeForm data."""

    __slots__ = []

    def setL1(self):
        self.set_generic_stacked_by_month(special_metric="sustainability_score")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month(special_metric="sustainability_score")

    def setTableName(self):
        self.table_name = "sus_field_staff_sustainability"


class MetricFromFieldStaffMOT(MetricInSustainabilityDash):
    """Subclass of MetricTF (and thus also MetricFromParameters)
    for metrics that can be pulled directly from TypeForm
    and get displayed as counts."""

    __slots__ = []

    def setL1(self):
        self.set_perma_mot()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_perma_mot()

    def setTableName(self):
        self.table_name = "sus_field_staff_sustainability"


class MetricSusCompliance(MetricInSustainabilityDash):
    __slots__ = []

    def setL1(self):
        self.set_sus_compliance_rate()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_sus_compliance_rate()

    def setTableName(self):
        self.table_name = "sus_field_staff_compliance"


class MetricBusDistance(MetricInSustainabilityDash):
    __slots__ = []

    def setL1(self):
        self.set_generic_stacked_by_month()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month()

    def setTableName(self):
        self.table_name = "sus_business_distances"


class MetricBusMOT(MetricInSustainabilityDash):
    def setL1(self):
        self.set_perma_mot()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_perma_mot()

    def setTableName(self):
        self.table_name = "sus_business_mot"


class MetricEmployeeDistance(MetricInSustainabilityDash):
    def setL1(self):
        self.set_generic_stacked_by_month()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month()

    def setTableName(self):
        self.table_name = "sus_employee_travel_distances"


class MetricEmpMOT(MetricInSustainabilityDash):
    def setL1(self):
        self.set_perma_mot()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_perma_mot()

    def setTableName(self):
        self.table_name = "sus_employee_mot"


class MetricEnergyUse(MetricInSustainabilityDash):
    def setL1(self):
        self.set_generic_stacked_by_month()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month()

    def setTableName(self):
        self.table_name = "sus_gas_electric_tracker"


class MetricSusTreesPlanted(MetricInSustainabilityDash):
    def generic_tree_sum(self):
        by = self.params.by
        metric = self.params.metric
        sql = (
            self.create_quarter_index()
            + f"""
        WITH base AS (
            SELECT  *
            FROM {self.table_name}
            WHERE quarter IN ({self.make_quarters_list()}){self.params.filters_string}
        ), grouped AS (
            SELECT  SUM({metric}) AS {metric},
                    COUNT({metric}) AS size,
                    quarter{by}
            FROM base
            GROUP BY quarter{by}
        )
        SELECT  tis.date_str AS quarter,
                COALESCE({metric}, 0) AS {metric}, 
                COALESCE(size, 0) AS size
        FROM date_index_{self.ui} tis
        LEFT JOIN grouped g ON tis.date_str = g.quarter
        ORDER BY quarter DESC{by}
        """
        )
        self.make_df(sql)

    def setL1(self):
        self.set_generic_stacked_by_month(
            month_func=self.generic_stack_handle_trees_planted_quarter
        )
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month(
            month_func=self.generic_stack_handle_trees_planted_quarter
        )

    def setTableName(self):
        self.table_name = "sus_trees_planted"


class MetricBusCFScore(MetricInSustainabilityDash):
    __slots__ = []

    def setL1(self):
        self.set_generic_stacked_by_month(special_metric="sustainability_score")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month(special_metric="sustainability_score")

    def setTableName(self):
        self.table_name = "sus_business_cfs"


class MetricCombinedCFScore(MetricInSustainabilityDash):
    __slots__ = []

    def setL1(self):
        self.set_generic_stacked_by_month(special_metric="sustainability_score")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_generic_stacked_by_month(special_metric="sustainability_score")

    def setTableName(self):
        self.table_name = "sus_total_cfs"


class MetricProjectCount(MetricComplianceDash):
    def setAllDates(self):
        self.setAllDatesCD(self.params.metric)

    def setMetric(self):
        core = """
                SELECT *{year_select}{month_select}{week_select}{dow_select}
                FROM base_project_count
                WHERE date >= '{startDate}'
                AND date <= '{endDate}'{filter_wc}
                ORDER BY date"""
        core = self.fill_sql_inserts(core)
        if self.params.layer == "csv":
            df = self.make_df(core)
            self.return_dict = df.to_csv(index=False)
            return
        elif self.params.layer == "1":
            sql = (
                self.create_month_index()
                + """
        SELECT  COUNT(DISTINCT project) AS project_count, 
                d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN ({core}) b ON d.date_str = b.month
        GROUP BY date_str
        ORDER BY date_str 
        ;"""
            )
        elif self.params.layer == "2":
            sql = """
        SELECT  COUNT(DISTINCT project) AS project_count
        FROM (  {core})
        ;"""
            if self.params.by != "":
                sql = """
        SELECT  COUNT(DISTINCT project) AS project_count{by}
        FROM (  {core})
        GROUP BY {sby}
        ;"""
        sql = self.fill_sql_inserts(sql, {"core": core})
        self.make_df(sql)


class MetricInSFRevenue(MetricBasic):
    __slots__ = []

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT 1 AS grouping_col, value, region, month, year
            FROM {table_name}
            WHERE stage = 'Closed Won'
            AND value_types = 'Sum of Amount'
            AND date >= '{startDate}'
            AND date <= '{endDate}'{filter_wc}
        )"""
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base;")
            self.return_dict = df.to_csv(index=False)
            return
        if self.params.layer == "1":
            sql = (
                self.create_month_index()
                + sql
                + """
        SELECT SUM(value::INTEGER) AS {col}, COUNT(value) AS size, d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN base b ON b.month = d.date_str 
        GROUP BY d.date_str
        ORDER BY d.date_str
        LIMIT 2
        ;"""
            )
        else:
            sql = (
                sql
                + """
        SELECT SUM(value::INTEGER) AS {col}, COUNT(value) AS size{by}{target}
        FROM base
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
            )
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setTableName(self):
        self.table_name = "sf_impact_api_integration_report_2"


class MetricInSFStatus(MetricBasic):
    __slots__ = []

    def setTableName(self):
        self.table_name = "sf_impact_project_status_report"


class MetricInSFProjectsNotSold(MetricInSFStatus):
    __slots__ = []

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col,
                    include_impact, 
                    CASE WHEN LEFT(include_impact, 3)  = 'Yes' THEN 1 ELSE 0 END AS i,
                    CASE WHEN LEFT(include_impact, 3) != 'Yes' THEN 1 ELSE 0 END AS ni,
                    region, month, year
            FROM {table_name}
            WHERE stage = 'Closed Won'
            AND owner != 'Subtotal'
            AND date >= '{startDate}'
            AND date <= '{endDate}'{filter_wc}
        )
        """
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base ORDER BY month;")
            self.return_dict = df.to_csv(index=False)
            return
        if self.params.layer == "1":
            sql = (
                self.create_month_index()
                + sql
                + """
        SELECT  SUM(i) AS ncldd, --included
                SUM(ni) AS not_ncldd,
                ROUND(100.0 * not_ncldd / (ncldd + not_ncldd), 1) AS projects_not_sold,
                COUNT(include_impact) AS size, 
                d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN base b ON b.month = d.date_str
        GROUP BY d.date_str
        ORDER BY d.date_str
        LIMIT 2
        ;"""
            )
        else:
            sql = (
                sql
                + """
        SELECT  SUM(i) AS ncldd, 
                SUM(ni) AS not_ncldd,
                ROUND(100.0 * not_ncldd / (ncldd + not_ncldd), 1) AS projects_not_sold,
                COUNT(include_impact) AS size{by}
        FROM base 
        GROUP BY grouping_col{by} 
        ORDER BY size DESC{by}
        ;"""
            )
        sql = sql.format(ui=self.ui, by=self.params.by)
        df = self.make_df(sql)


class MetricInSFRevenueRelated(MetricInSFStatus):
    __slots__ = []

    def setRevenue(self, lost: bool = True, multiplier: float = 0.05) -> None:
        include_impact = ""
        if lost:
            include_impact = "!"
        sql = """
        WITH base AS (
            SELECT  revenue, 
                    SPLIT_PART(revenue, ' ', 1) AS currency,
                    REPLACE(SPLIT_PART(revenue, ' ', 2), ',', '')::FLOAT AS rev_num,
                    rev_num * {multiplier} AS {col},
                    region, month, year
            FROM {table_name}
            WHERE stage = 'Closed Won'
            AND owner != 'Subtotal'
            AND LEFT(include_impact, 3) {include_impact}= 'Yes'
            AND date >= '{startDate}'
            AND date <= '{endDate}'{filter_wc}
        ), converted AS (
            SELECT  b.*, 
                    e.rate, 
                    b.{col} * e.rate AS {col}_in_gbp 
            FROM base b
            LEFT JOIN exchange_rates e ON e.gbp_to = b.currency
        )"""
        sql = self.fill_sql_inserts(
            sql,
            {
                "multiplier": multiplier,
                "include_impact": include_impact,
            },
        )
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM converted;")
            self.return_dict = df.to_csv(index=False)
            return
        if self.params.layer == "1":
            sql = (
                self.create_month_index()
                + sql
                + """
        SELECT  ROUND(SUM({col}), 2) AS {col}, 
                COUNT({col}) AS size, 
                'converted to GBP' AS currency,
                d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN converted c ON c.month = d.date_str
        GROUP BY d.date_str
        ORDER BY d.date_str
        LIMIT 2
        ;"""
            )
        else:
            sql = (
                sql
                + """
        , dummied AS (
            SELECT 1 AS dummy, c.*
            FROM converted c
        )
        SELECT ROUND(SUM({col}), 2) AS {col}, COUNT({col}) AS size{by}{target}
        FROM dummied
        GROUP BY dummy{by}
        ORDER BY dummy{by}
        ;"""
            )
        sql = self.fill_sql_inserts(sql)
        df = self.make_df(sql)


class MetricInSFRevenueLost(MetricInSFRevenueRelated):
    __slots__ = []

    def setMetric(self) -> None:
        self.setRevenue(lost=True, multiplier=0.05)


# class MetricInSFRevenue(MetricInSFRevenueRelated):
#     __slots__ = []

#     def setMetric(self) -> None:
#         self.setRevenue(lost=False, multiplier=1)


class MetricInSFDataCollection(MetricInSFStatus):
    __slots__ = []

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col,
                    data_collected, 
                    CASE WHEN data_collected = 'YES' THEN 0 ELSE 1 END AS nc,
                    CASE WHEN data_collected = 'YES' THEN 1 ELSE 0 END AS c,
                    region, month, year
            FROM {table_name}
            WHERE stage = 'Closed Won'
            AND owner != 'Subtotal'
            AND date >= '{startDate}'
            AND date <= '{endDate}'{filter_wc}
        )"""
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base ORDER BY month;")
            self.return_dict = df.to_csv(index=False)
            return
        if self.params.layer == "1":
            sql = (
                self.create_month_index()
                + sql
                + """
        SELECT  SUM(c) AS collected, 
                SUM(nc) AS not_collected,
                ROUND(100.0 * collected / (collected + not_collected), 1) 
                    AS data_collection,
                COUNT(data_collected) AS size,
                d.date_str AS month
        FROM date_index_{ui} d
        LEFT JOIN base b ON b.month = d.date_str
        GROUP BY d.date_str
        ORDER BY d.date_str
        LIMIT 2
        ;"""
            )
        else:
            sql = (
                sql
                + """
        SELECT data_collected AS data_collection, count, size{sdotby}
        FROM  ( SELECT data_collected, COUNT(grouping_col) AS count{by}
                FROM base
                GROUP BY data_collected{by} ) m
        JOIN  ( SELECT 1 AS dummy, SUM(grouping_col) AS size{by}
                FROM base
                GROUP BY dummy{by} ) s ON TRUE{byjoin}
        ORDER BY count{sdotby}
        ;"""
            )
        if self.params.sby == "":
            sdotby = ""
            byjoin = ""
        else:
            sdotby = f", s.{self.params.sby}"
            byjoin = f" AND s.{self.params.sby} = m.{self.params.sby}"
        sql = self.fill_sql_inserts(sql, {"sdotby": sdotby, "byjoin": byjoin})
        df = self.make_df(sql)


class MetricForUberEats(MetricFromParameters):
    __slots__ = []

    def setAllDates(self):
        self.setAllDatesSimple("uber_applies_redemption", include_job_filter=False)

    def setFullExport(self):
        sql = """
        SELECT  *
        FROM uber_applies_redemption
        WHERE date >= '{startDate}'
          AND date <= '{endDate}'
        ORDER BY location
        ;"""
        sql = sql.format(startDate=self.params.startDate, endDate=self.params.endDate)
        df = self.make_df(sql)
        self.return_dict = df.to_csv(index=False)
        self.useS3("csv", use_json=True)

    def getUberSum(self, agg):
        if agg == "avg":
            agg_str = f"AVG({self.params.metric}::INTEGER* 1.0) AS {self.params.metric}"
        elif agg == "sum":
            agg_str = f"SUM({self.params.metric}::INTEGER) AS {self.params.metric}"
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    applies, 
                    ftes, 
                    promo_code{h2_select}{week_select}{dow_select}
            FROM uber_applies_redemption
            WHERE date >= '{startDate}'
                AND date <= '{endDate}'{filter_wc}{job_filter}
        )
        SELECT  {agg_str}, COUNT(value) AS size{by}{target}
        FROM (  SELECT grouping_col,{col}, MAX({col}) AS value{by}
                FROM base
                GROUP BY grouping_col,{col}{by}) 
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql, {"agg_str": agg_str})
        self.make_df(sql)

    def getUberFTEApplies(self):
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    applies, 
                    ftes, 
                    promo_code{h2_select}{week_select}{dow_select}
            FROM uber_applies_redemption
            WHERE date >= '{startDate}'
                AND date <= '{endDate}'{filter_wc}{job_filter}
        )
        SELECT  ROUND((SUM(ftes)/SUM(applies)::FLOAT)*100,2) as {col},
                COUNT(applies) AS size{by}{target}
        FROM base
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def getUberCostPer(self):
        metric = self.params.metric
        if metric[-1] == "e":
            col = "ftes"
        if metric[-1] == "y":
            col = "applies"
        weekly_cost = self.job_metric_dict[self.params.job][metric]["weekly_cost"]
        total_costs = f"""(COUNT({col})*{weekly_cost})"""
        sql = """
        WITH base AS (
            SELECT  1 AS grouping_col, 
                    applies, 
                    ftes, 
                    promo_code{h2_select}{week_select}{dow_select}
            FROM uber_applies_redemption
            WHERE date >= '{startDate}'
                AND date <= '{endDate}'{filter_wc}{job_filter}
        )
        SELECT  ROUND(COALESCE({total_costs} / NULLIF(SUM({col}), 0), {total_costs}), 2)
                    AS {metric},
                COUNT({col}) AS size, grouping_col{by}{target}
        FROM base
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;"""
        sql = self.fill_sql_inserts(
            sql,
            {
                "col": col,
                "metric": metric,
                "total_costs": total_costs,
                "weekly_cost": weekly_cost,
            },
        )
        self.make_df(sql)

    def getUberTFRatio(self):
        filter_by_form_id = f"""
            AND form_id IN ({jnInserter(getParameterFromJMD("form_id", self.params),dropQ=True)})"""
        if (
            self.params.metric[-1] == "e"
        ):  # added so same method could be used on both fte and apply metrics
            col = "ftes"
        if self.params.metric[-1] == "y":
            col = "applies"
        cdrg_by = ""
        fdg_join = ""
        if self.params.sby != "":
            sby = self.params.sby
            cdrg_by = f", cdrg.{sby}"
            fdg_join = f""" AND fdg.{sby} = cdrg.{sby}"""

        sql = """
        WITH coupon_distributed_redeemed AS (
            SELECT 1 AS grouping_col,
                promo_code,
                {col}{dow_select}{week_select}{h2_select}
            FROM uber_applies_redemption
            WHERE date >= '{startDate}'
                AND date <= '{endDate}'{job_filter}{filter_wc}         
        ), flyers_distributed AS (
            SELECT 1 as grouping_col, value{full_select_insert}
            FROM tf_queries m
            WHERE field_ref LIKE 'flyers_distributed%%'{filter_by_form_id}
        ), cdrg AS (
            SELECT SUM({col}) AS {col}, COUNT({col}) AS size{by}
            FROM coupon_distributed_redeemed cdr
            GROUP BY grouping_col{by}
        ), fdg AS (
            SELECT SUM(value) AS flyers_distributed{by}
            FROM flyers_distributed
            GROUP BY grouping_col{by}
        ), base AS (
            SELECT {col}, size, flyers_distributed{cdrg_by}
            FROM cdrg
            JOIN fdg ON TRUE{fdg_join}
        )
        SELECT  ROUND(({col}/flyers_distributed::FLOAT)*100,2) AS {metric}, 
                size{by}{target}
        FROM  base   
        """
        sql = self.fill_sql_inserts(
            sql,
            {
                "col": col,
                "metric": self.params.metric,
                "filter_by_form_id": filter_by_form_id,
                "fdg_join": fdg_join,
                "cdrg_by": cdrg_by,
            },
        )
        self.make_df(sql)


class MetricUberSum(MetricForUberEats):
    def setL1(self):
        self.getUberSum("sum")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.getUberSum("sum")


class MetricUberRatio(MetricForUberEats):
    def setL1(self):
        self.getUberFTEApplies()
        self.l1 = L1IndicatorGetter(self, digits=2)

    def setL2(self):
        self.getUberFTEApplies()


class MetricUberCostPer(MetricForUberEats):
    # cost_fte l2 off by 3 orders of magnitude

    def setL1(self):
        self.getUberCostPer()
        self.l1 = L1IndicatorGetter(self, digits=2)

    def setL2(self):
        self.getUberCostPer()


class MetricUberTFRatio(MetricForUberEats):
    def setL1(self):
        self.getUberTFRatio()
        self.l1 = L1IndicatorGetter(self, digits=2)

    def setL2(self):
        self.getUberTFRatio()


class MetricRevenuePerLead(MetricBasic):
    __slots__ = []

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT  project_id,
                    customer_id,
                    gender,
                    age_group,
                    persona,
                    months_after,
                    field_ref,
                    value{product_select}{h1_select}
            FROM {table_name} m{h1_join}
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'
              AND field_ref = 'total_sales_value'{filter_wc}{job_filter}
        )
        """
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base;")
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        sql = (
            sql
            + """
        , by_wave AS (
            SELECT  MAX(value) AS value,
                    customer_id{by}
            FROM base
            GROUP BY customer_id{by}
        )
        """
        )
        if self.params.by == "":
            sql = (
                sql
                + """
        SELECT  SUM(value) AS revenue,
                COUNT(DISTINCT customer_id) AS size,
                ROUND(1.0 * revenue / size, 2) AS revenue_per_lead{target}
        FROM by_wave
        """
            )
        else:  # if self.params.by != "":
            sql = (
                sql
                + """
        SELECT  SUM(value) AS revenue,
                COUNT(DISTINCT customer_id) AS size,
                ROUND(1.0 * revenue / size, 2) AS revenue_per_lead{by}{target}
        FROM by_wave
        GROUP BY {sby}
        ORDER BY {sby}
        ;"""
            )
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setTableName(self):
        self.table_name = "tf_post_tracking"


class MetricRevenuePerLeadDemo(MetricRevenuePerLead):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_post_tracking"


class MetricRepeatPurchaseRate(MetricBasic):
    __slots__ = []

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT  project_id,
                    customer_id,
                    product_type::VARCHAR,
                    gender,
                    age_group,
                    persona,
                    months_after,
                    TRUNC(date)::VARCHAR AS date,
                    'repeat_purchase' AS field_ref,
                    CASE WHEN date = FIRST_VALUE (date) OVER (
                        PARTITION BY customer_id ORDER BY date ROWS 
                        BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                         THEN 0 ELSE 1 END AS value
            FROM {table_name} m
            JOIN m.pt AS product_type ON TRUE
            WHERE field_ref = 'total_sales_value'
        ), filtered AS (
            SELECT *{h1_select} 
            FROM base m
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}{job_filter}
        )
        """  # Any field_ref will do, but need to pick one so we don't dupe.
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM filtered;")
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        if self.params.by == "":
            sql = (
                sql
                + """
        , maxxed AS (
            SELECT MAX(value) AS max, customer_id
            FROM filtered
            GROUP BY customer_id
        )
        SELECT  ROUND(100.0 * SUM(max) / COUNT(customer_id), 1) AS repeat_purchase_rate,
                COUNT(customer_id) AS size{target}
        FROM maxxed
        ;
        """
            )
        elif self.params.by == ", months_after":
            sql = (
                sql
                + """
        , maxxed AS (
            SELECT MAX(value) AS max, customer_id, months_after
            FROM filtered
            GROUP BY customer_id, months_after
        ), groupings AS (
            SELECT c.customer_id, m.months_after
            FROM (SELECT DISTINCT months_after FROM maxxed) m
            CROSS JOIN (SELECT DISTINCT customer_id FROM maxxed) c
        ), ready AS (
            SELECT COALESCE(m.max, 0) AS max, g.customer_id, g.months_after
            FROM groupings g
            LEFT JOIN maxxed m 
                   ON g.months_after = m.months_after AND g.customer_id = m.customer_id
        )
        -- SELECT *
        -- FROM ready
        -- ORDER BY months_after, customer_id
        SELECT  ROUND(100.0 * SUM(max) / COUNT(customer_id), 1) AS repeat_purchase_rate,
                COUNT(customer_id) AS size, 
                months_after{target}
        FROM ready
        GROUP BY months_after
        ORDER BY months_after
        ;"""
            )
        else:  # if self.params.by != "":
            sql = (
                sql
                + """
        , maxxed AS (
            SELECT MAX(value) AS max, customer_id{by}
            FROM filtered
            GROUP BY customer_id{by}
        )
        SELECT  ROUND(100.0 * SUM(max) / COUNT(customer_id), 1) AS repeat_purchase_rate,
                COUNT(customer_id) AS size{by}{target}
        FROM maxxed
        GROUP BY {sby}
        ORDER BY {sby}
        ;"""
            )
        sql = self.fill_sql_inserts(sql)
        self.make_df(sql)

    def setTableName(self):
        self.table_name = "tf_post_tracking"


class MetricRepeatPurchaseRateDemo(MetricRepeatPurchaseRate):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_post_tracking"


class MetricCustomerAcquisition(MetricWTableName):
    def setCAC(self):
        params = self.params
        cost = getParameterFromJMD("cost", params)
        sql = f"""
        WITH base AS (
            SELECT CASE WHEN TRIM(value) = 'Yes' THEN 1 ELSE 0 END AS value
            FROM tf_demo_post_tracking
            WHERE field_ref = 'new_customer_acquired'
        )
        SELECT ROUND({cost}* 1.0/ SUM(value), 2) AS customer_acquisition{params.target}
        FROM base
        ;
        """
        self.make_df(sql)

    def setL1(self):
        self.setCAC()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        return  # Never used since only L1 in use.

    def setFullExport(self):
        return  # Never used since only L1 in use.

    def setAllDates(self):
        return  # Never used since only L1 in use.

    def setTableName(self):
        self.table_name = "tf_post_tracking"


class MetricCustomerAcquisitionDemo(MetricCustomerAcquisition):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_post_tracking"


# age_group_order = """
# CREATE TABLE age_group_order (
#     id INT,
#     age_group VARCHAR(20)
# );
# INSERT INTO age_group_order (id, age_group)
# VALUES
#     (2, '18-24 Years'),
#     (3, '25-34 Years'),
#     (4, '35-44 Years'),
#     (5, '45-64 Years'),
#     (1, '<18 Years'),
#     (6, '>65 Years');
# """


class MetricCustomerLifetimeValue(MetricBasic):
    __slots__ = []

    def setMetric(self):
        lifetime = 3.916455696202532
        sql = """
        WITH base AS (
            SELECT  project_id,
                    t.customer_id,
                    product_type::VARCHAR,
                    gender,
                    age_group,
                    persona,
                    months_after,
                    TRUNC(date)::VARCHAR AS date,
                    field_ref,
                    value::FLOAT
            FROM {table_name} t 
            JOIN t.pt AS product_type ON TRUE
            WHERE field_ref = 'total_sales_value'
        ), customers AS (
            SELECT DISTINCT customer_id
            FROM base
            WHERE value > 0
            GROUP BY customer_id
        ), filtered AS (
            SELECT  m.*{h1_select} 
            FROM customers c
            LEFT JOIN base m ON m.customer_id = c.customer_id
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}{job_filter}
        )
        """
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM filtered;")
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        else:
            sby = self.params.sby
            by = ""
            if sby != "":
                by = f""", 
                    a.{sby}"""
            by_order = by
            by_join = ""
            if sby == "age_group":
                by_order = """,
                    o.id"""
                by_join = f"""
            JOIN age_group_order o ON a.{sby} = o.{sby}"""
            sql = (
                sql
                + f"""
        , averaged AS (
            SELECT  1 AS dummy,
                    SUM(value*1.0) AS sm, 
                    COUNT(value) AS cnt, 
                    customer_id, 
                    sm/cnt AS avg{by}
            FROM filtered a
            GROUP BY customer_id{by}
        )
        SELECT  SUM(sm) AS total, 
                SUM(cnt) AS size, 
                1.0 * total / size AS averaged, 
                ROUND(averaged * {lifetime}, 2) AS customer_lifetime_value{by}
        FROM averaged a{by_join}
        GROUP BY dummy{by}{by_order}
        ORDER BY dummy{by_order}
        ;"""
            )
        self.make_df(sql)

    def setTableName(self):
        self.table_name = "tf_post_tracking"


class MetricCustomerLifetimeValueDemo(MetricCustomerLifetimeValue):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_post_tracking"


class MetricCustomerChurnRate(MetricBasic):
    __slots__ = []

    def setMetric(self):
        sql = """
        WITH base AS (
            SELECT  project_id,
                    customer_id,
                    product_type::VARCHAR,
                    gender,
                    age_group,
                    persona,
                    months_after,
                    TRUNC(date)::VARCHAR AS date,
                    field_ref
            FROM {table_name} m
            JOIN m.pt AS product_type ON TRUE
            WHERE field_ref = 'number_products_purchased'
              AND value > 0
        ), filtered AS (
            SELECT *{h1_select} 
            FROM base m
            WHERE date >= '{startDate}'
              AND date <= '{endDate}'{filter_wc}{job_filter}
        )"""
        sql = self.fill_sql_inserts(sql)
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM filtered;")
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        if self.params.layer == "1":
            sql = (
                sql
                + """
        , m3 AS (
            SELECT COUNT(DISTINCT customer_id) AS cst
            FROM filtered
            WHERE months_after = 3
        ), m6 AS (
            SELECT COUNT(DISTINCT customer_id) AS cst
            FROM filtered
            WHERE months_after = 6
        )
        SELECT  m3.cst AS m3, m6.cst AS m6, 
                100 - 100.0 * m6.cst/m3.cst AS customer_churn_rate
        FROM m3 JOIN m6 ON TRUE
        """
            )
        elif self.params.layer == "2":
            by = self.params.sby
            if by == "":
                sql = (
                    sql
                    + """
        SELECT months_after AS month, COUNT(DISTINCT customer_id) AS customer_churn_rate
        FROM filtered
        GROUP BY months_after
        ORDER BY months_after
        ;"""
                )
            else:
                # l2-by
                sql = (
                    sql
                    + f"""
        , groupings AS (
            SELECT  DISTINCT t.{by}, 
                    DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT customer_id) DESC) AS rnk
            FROM filtered t
            GROUP BY t.{by}
        ), months AS (
            SELECT DISTINCT months_after
            FROM filtered
        ), crossed AS (
            SELECT months_after, {by}, rnk
            FROM months
            CROSS JOIN groupings
        )
        SELECT  c.months_after AS month, c.{by}, rnk,
                COUNT(DISTINCT customer_id) AS customer_churn_rate
        FROM crossed c
        LEFT JOIN filtered t ON t.months_after = c.months_after AND t.{by} = c.{by}
        GROUP BY c.months_after, c.rnk, c.{by} 
        ORDER BY c.months_after, c.rnk
        ;"""
                )
        self.make_df(sql)

    def setTableName(self):
        self.table_name = "tf_post_tracking"


class MetricCustomerChurnRateDemo(MetricCustomerChurnRate):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_post_tracking"


class MetricInStockLevels(MetricTF):
    __slots__ = []

    def setL1(self):
        self.getStockLevels()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.getStockLevels()

    def setFullExport(self):
        self.getStockLevels()


class MetricSamplesPerEngagement(MetricTF):
    __slots__ = []

    def setL1(self):
        self.setSamplesPerEngagement()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.setSamplesPerEngagement()

    def setFullExport(self):
        self.setSamplesPerEngagement()


class MetricLeadsConverted(MetricTF):
    __slots__ = []

    def setL1(self):
        self.setLeadsConverted()
        self.l1 = L1IndicatorGetter(self, digits=2)

    def setL2(self):
        self.setLeadsConverted()

    def setFullExport(self):
        self.setLeadsConverted()


class MetricInKaidu(MetricBasic):
    __slots__ = []

    def setTableName(self):
        self.table_name = "kaidu"  # Not needed.

    def get_hours_open(self):
        hours_open_dict = {
            "TTG01001": """'08:00', '09:00', '10:00', '11:00', '12:00',
                            '13:00', '14:00', '15:00', '16:00', '17:00'""",
            "THT01003": """'08:00', '09:00', '10:00', '11:00', '12:00',
                            '13:00', '14:00', '15:00', '16:00', '17:00',
                            '18:00', '19:00', '20:00'""",
        }
        return hours_open_dict[self.params.job]


class MetricInKaiduEventTraffic(MetricInKaidu):
    __slots__ = []

    def setMetric(self):
        startDate = self.params.startDate
        endDate = self.params.endDate
        # Do not include filter_wc;
        # stacked-bar-chart does not handle non-date-range filters.
        job_filter = self.params.job_filter
        hours_open = self.get_hours_open()
        sql = f"""
        WITH base AS (
            SELECT  customer_id,
                    TO_CHAR(date, 'YYYY-MM-DD') AS date,
                    tz::VARCHAR,
                    zone,
                    hour,
                    type,
                    value::INTEGER,
                    day_of_week
            FROM kaidu
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'
              AND type = 'count'{job_filter}
              AND hour IN ( {hours_open}, 'all')
              AND tenmin = ''{job_filter}
        )"""
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base;")
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        metric = self.params.metric
        if self.params.layer == "1":
            sql = (
                sql
                + f"""
        SELECT  SUM(value) AS {metric}, 
                '{startDate}' AS start_date, 
                '{endDate}' AS end_date
        FROM base
        WHERE hour = 'all'
          AND zone = 'all'
        ;
        """
            )
        target = self.params.target
        if self.params.layer == "2":
            if self.params.by == "":
                sql = (
                    sql
                    + f"""
        SELECT  value AS {metric}, date{target}
        FROM base
        WHERE hour = 'all'
          AND zone = 'all'
        ORDER BY date
        ;
        """
                )
            else:
                sql = (
                    self.create_date_index()
                    + sql
                    + f"""
        , days AS (
            SELECT date_str AS date
            FROM date_index_{self.ui}
        )"""
                )
                sby = self.params.sby
                hour_not = ""
                zone_not = ""
                rnk = f"DENSE_RANK () OVER (ORDER BY {sby}) AS rnk"
                if self.params.by == ", hour":
                    hour_not = "!"
                elif self.params.by == ", zone":
                    zone_not = "!"
                else:
                    rnk = "DATE_PART('dayofweek', date::DATE)::INTEGER + 1 AS rnk"
                sql = (
                    sql
                    + f"""
        , {sby}s AS (
            SELECT  DISTINCT {sby}, 
                    {rnk}
            FROM base
            WHERE {sby} != 'all'
        ), {sby}days AS (
            SELECT date, {sby}, rnk
            FROM {sby}s
            CROSS JOIN days
        )
        SELECT  COALESCE(value, 0) AS {metric}, h.date, h.{sby}, rnk{target}
        FROM {sby}days h
        LEFT JOIN ( SELECT value, date, {sby}
                    FROM base 
                    WHERE hour {hour_not}= 'all'
                      AND zone {zone_not}= 'all' ) m 
               ON h.{sby} = m.{sby} AND h.date = m.date
        ORDER BY h.date, rnk
        ;"""
                )
        self.make_df(sql)


class MetricInKaiduHFH(MetricInKaidu):
    __slots__ = []

    def setMetric(self):
        startDate = self.params.startDate
        endDate = self.params.endDate
        filter_wc = self.params.filters_string
        job_filter = self.params.job_filter
        hours_open = self.get_hours_open()
        sql = f"""
        WITH base AS (
            SELECT  customer_id,
                    TO_CHAR(date, 'YYYY-MM-DD') AS date,
                    tz::VARCHAR,
                    zone,
                    hour,
                    type,
                    value::INTEGER,
                    day_of_week
            FROM kaidu
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'
              AND value != 0
              AND type = 'count'
              AND hour IN ( {hours_open})
              AND tenmin = ''{job_filter}{filter_wc}
        )
        """
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base;")
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        metric = self.params.metric
        by = self.params.by
        if self.params.layer == "1":
            sql = (
                sql
                + f"""
        SELECT  value AS size, 
                SUM(value) OVER () AS total, 
                100 * size/total AS indicator_value, 
                'green' AS indicator_colour,
                CHR(37)||' of all hours:' AS indicator_label, 
                hour AS {metric}
        FROM base
        WHERE zone = 'all'
        ORDER BY size DESC
        LIMIT 1
        ;
        """
            )
        if self.params.layer == "2":
            zone_not_insert = ""
            if by == ", zone":
                zone_not_insert = "!"
            sql = (
                sql
                + f"""
        SELECT  hour AS {metric}, 
                SUM(value) AS count, 
                COUNT(value) AS size{by}
        FROM base
        WHERE zone {zone_not_insert}= 'all'
        GROUP BY {metric}{by}
        ORDER BY {metric}{by}
        ;
        """
            )  # Yes, those sum/count mixes are correct.  :S
        self.make_df(sql)


class MetricInKaiduBZ(MetricInKaidu):
    __slots__ = []

    def setMetric(self):
        startDate = self.params.startDate
        endDate = self.params.endDate
        filter_wc = self.params.filters_string
        job_filter = self.params.job_filter
        hours_open = self.get_hours_open()
        sql = f"""
        WITH base AS (
            SELECT  customer_id,
                    TO_CHAR(date, 'YYYY-MM-DD') AS date,
                    tz::VARCHAR,
                    zone,
                    hour,
                    type,
                    value::INTEGER,
                    day_of_week
            FROM kaidu
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'
              AND value != 0
              AND type = 'count'
              AND zone != 'all' AND zone != ''
              AND hour IN ( {hours_open}, 'all')
              AND tenmin = ''{job_filter}{filter_wc}
        )
        """
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base;")
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        metric = self.params.metric
        by = self.params.by
        if self.params.layer == "1":
            sql = (
                sql
                + f"""
        SELECT  value AS size, 
                SUM(value) OVER () AS total, 
                100 * size/total AS indicator_value, 
                'green' AS indicator_colour,
                CHR(37)||' of all zones:' AS indicator_label, 
                zone AS {metric}
        FROM base
        WHERE hour = 'all'
        ORDER BY size DESC
        LIMIT 1
        ;
        """
            )
        if self.params.layer == "2":
            hour_not_insert = ""
            if by == ", hour":
                hour_not_insert = "!"
            sql = (
                sql
                + f"""
        SELECT  zone AS {metric}, 
                SUM(value) AS count, 
                COUNT(value) AS size{by}
        FROM base
        WHERE hour {hour_not_insert}= 'all'
        GROUP BY {metric}{by}
        ORDER BY {metric}{by}
        ;
        """
            )  # Yes, those sum/count mixes are correct.  :S
        self.make_df(sql)


class MetricInKaiduDT(MetricInKaidu):
    __slots__ = []

    def setMetric(self):
        startDate = self.params.startDate
        endDate = self.params.endDate
        filter_wc = self.params.filters_string
        job_filter = self.params.job_filter
        hours_open = self.get_hours_open()
        sql = f"""
        WITH base AS (
            SELECT  customer_id,
                    TO_CHAR(date, 'YYYY-MM-DD') AS date,
                    tz::VARCHAR,
                    zone,
                    hour,
                    type,
                    value,
                    day_of_week
            FROM kaidu
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'
              AND hour IN ( {hours_open}, 'all')
              AND zone NOT IN ( 'Exit Left', 'Exit Right', 
                                'Shop Entrance Left', 'Shop Entrance Right')
              AND tenmin = ''{job_filter}{filter_wc}
        )
        """
        if self.params.layer == "csv":
            df = self.make_df(sql + " SELECT * FROM base;")
            self.return_dict = df.to_csv(index=False)
            self.useS3("csv", use_json=True)
            return
        metric = self.params.metric
        sby = self.params.sby
        by = self.params.by
        non_zone_by = by
        non_zone_sby = sby
        t_by_insert = ""
        if self.params.sby == "zone":
            non_zone_by = ""
            non_zone_sby = ""
        if self.params.by != "":
            t_by_insert = f", dt.{sby}"
        hour_not_insert = ""
        if self.params.sby == "hour":
            hour_not_insert = "!"
        zone_not_insert = ""  # No longer available.
        # if self.params.sby == 'zone':
        #     zone_not_insert = "!"
        by_join = ""
        if non_zone_by != "":
            by_join = f" AND dt.{non_zone_sby} = cnt.{non_zone_sby}"
        sql = (
            sql
            + f"""
        , single AS (
            SELECT  value, type, date, zone{non_zone_by}
            FROM base
            WHERE hour {hour_not_insert}= 'all'
              --AND zone {zone_not_insert}= 'all' --No longer available.
        ), dt AS (
            SELECT value AS {metric}, date, zone{non_zone_by} 
            FROM single 
            WHERE type = 'dwell_time'
        ), cnt AS (
            SELECT value::INTEGER AS size, date, zone{non_zone_by} 
            FROM single 
            WHERE type = 'count'
        ), total AS (
            SELECT  {metric} * size AS total_dt, 
                    size AS sze, 
                    1 AS grouping_col{t_by_insert}
            FROM dt
            JOIN cnt ON dt.date = cnt.date AND dt.zone = cnt.zone{by_join}
        )
        SELECT  SUM(sze) AS size, 
                CASE WHEN size = 0 THEN 0 
                     ELSE ROUND(1.0 * SUM(total_dt) / size, 1) END AS {metric}{by} 
        FROM total
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;
        """
        )
        self.make_df(sql)


class MetricSquare(MetricBasic):
    __slots__ = []

    def setTableName(self):
        self.table_name = "square_o365"


class MetricProductsSoldSquare(MetricSquare):
    def setMetric(self):
        metric = self.params.metric
        by = self.params.by
        startDate = self.params.startDate
        endDate = self.params.endDate
        filter_wc = self.params.filters_string
        job_filter = self.params.job_filter
        table_name = self.table_name
        sql = f"""
        WITH base AS (
            SELECT  project_id,
                    date,
                    "time",
                    time_zone,
                    qty{by}
            FROM {table_name} m
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'{job_filter}{filter_wc}
        )
        """
        sql = sql.format()
        if self.params.layer == "csv":
            self.handleCSV(sql)
            return
        if self.params.layer == "1":
            sql = (
                sql
                + f"""
        SELECT  SUM(qty) AS {metric}
        FROM base
        ;
        """
            )
        if self.params.layer == "2":
            sql = (
                sql
                + f"""
        , total AS (
            SELECT  qty, 1 AS grouping_col{by}
            FROM base
        )
        SELECT SUM(qty) AS {metric}, COUNT(qty) AS size{by} 
        FROM total
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;
        """
            )
        self.make_df(sql)


class MetricSalesValueSquare(MetricSquare):
    def setL1(self):
        self.setMetric()
        self.l1 = L1IndicatorGetter(self, digits=2)

    def setMetric(self):
        metric = self.params.metric
        by = self.params.by
        startDate = self.params.startDate
        endDate = self.params.endDate
        filter_wc = self.params.filters_string
        job_filter = self.params.job_filter
        table_name = self.table_name
        sql = f"""
        WITH base AS (
            SELECT  date,
                    "time",
                    time_zone,
                    product_sales{by}
            FROM {table_name} m
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'{job_filter}{filter_wc}
        )
        """
        sql = sql.format()
        if self.params.layer == "csv":
            self.handleCSV(sql)
            return
        if self.params.layer == "1":
            sql = (
                sql
                + f"""
        SELECT  ROUND(SUM(product_sales), 2) AS {metric}
        FROM base
        ;
        """
            )
        if self.params.layer == "2":
            sql = (
                sql
                + f"""
        , total AS (
            SELECT  product_sales, 1 AS grouping_col{by}
            FROM base
        )
        SELECT  ROUND(SUM(product_sales), 2) AS {metric},
                COUNT(product_sales) AS size{by}
        FROM total
        GROUP BY grouping_col{by}
        ORDER BY grouping_col{by}
        ;
        """
            )
        self.make_df(sql)


class MetricPopularBuyingCategorySquare(MetricSquare):
    def setMetric(self):
        metric = self.params.metric
        by = self.params.by
        startDate = self.params.startDate
        endDate = self.params.endDate
        filter_wc = self.params.filters_string
        job_filter = self.params.job_filter
        table_name = self.table_name
        sql = f"""
        WITH base AS (
            SELECT  date,
                    "time",
                    time_zone,
                    category,
                    qty{by}
            FROM {table_name} m
            WHERE TRUNC(date) >= '{startDate}'
              AND TRUNC(date) <= '{endDate}'{job_filter}{filter_wc}
        )
        """
        sql = sql.format()
        if self.params.layer == "csv":
            self.handleCSV(sql)
            return
        sql = (
            sql
            + f"""
        SELECT category AS {metric}, count, size{by}
        FROM  ( SELECT category, SUM(qty) AS count{by}
                FROM base 
                GROUP BY category{by} 
                ORDER BY count DESC ) m
        JOIN  ( SELECT COUNT(category) AS size
                FROM base ) s ON TRUE
        ;
        """
        )
        self.make_df(sql)


class MetricTFTargetByGrouping(MetricTF):
    __slots__ = []

    def set_benchmark(self, agg_str):
        custom_dates = {
            "startDate": self.params.getMinDate(self.engine),
            "endDate": self.params.getMaxDate(self.engine),
        }
        self.genericAvgSum("avg", False, custom_dates)
        bench_df = self.df
        self.genericAvgSum(agg_str)
        self.df["target"] = bench_df[self.params.metric]

    def set_benchmark_engagement(self):
        custom_dates = {
            "startDate": self.params.getMinDate(self.engine),
            "endDate": self.params.getMaxDate(self.engine),
        }
        self.setEngagementScore(custom_dates)
        bench_df = self.df
        self.setEngagementScore()
        self.df["target"] = bench_df[self.params.metric]


class MetricByGroupingAverage(MetricTFTargetByGrouping):
    def setL1(self):
        self.genericAvgSum("avg")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_benchmark("avg")


class MetricByGroupingSum(MetricTFTargetByGrouping):
    def setL1(self):
        self.genericAvgSum("sum")
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_benchmark("sum")
        self.df["target"] = self.df["target"] * self.df["size"]


class MetricByGroupingEngagementScore(MetricTFTargetByGrouping):
    def setL1(self):
        self.setEngagementScore()
        self.l1 = L1IndicatorGetter(self)

    def setL2(self):
        self.set_benchmark_engagement()


class MetricCrmOc(MetricBasic):
    def setTableName(self):
        self.table_name = "base_onboarding_conversion"

    def setMetric(self):
        region_insert = ""
        groupby_insert = """
        GROUP BY region"""
        if self.params.sby != "region":
            region_insert = "'AT/UK/US' AS "
            groupby_insert = ""
        inserts = {"region_insert": region_insert, "groupby_insert": groupby_insert}
        sql = """
        WITH base AS (
            SELECT  id, registered, application_submitted, accepted, rejected, region, date
            FROM base_onboarding_conversion
            WHERE date >= '{startDate}'
            AND date <= '{endDate}'{filter_wc}
        )
        """
        if self.params.layer == "csv":
            sql = self.fill_sql_inserts(sql, inserts)
            self.handleCSV(sql)
            return
        elif self.params.layer == "1":
            sql = (
                sql
                + """
        , counted AS (
            SELECT  COUNT(registered) AS reg,
                    COUNT(application_submitted) AS app,
                    COUNT(accepted) AS acc,
                    COUNT(rejected) AS rej
            FROM base    
        )
        SELECT  ROUND(100.0 * acc/(reg + app + acc + rej), 1) AS onboarding_conversion,
                reg + app + acc + rej AS size{target}
        FROM counted
        ;"""
            )
            sql = self.fill_sql_inserts(sql)
            self.make_df(sql)
            return
        sql = (
            sql
            + """
        SELECT  COUNT(registered) AS registered,
                COUNT(application_submitted) AS application_submitted,
                COUNT(accepted) AS accepted,
                COUNT(rejected) AS rejected,
                {region_insert}region
        FROM base{groupby_insert}
        ;"""
        )
        sql = self.fill_sql_inserts(sql, inserts)
        self.make_df(sql)
        output = []
        for _, row in self.df.iterrows():
            for j in ["registered", "application_submitted", "accepted", "rejected"]:
                output.append({"from": j, "value": row[j], "region": row["region"]})
            output.append(
                {
                    "from": "registered",
                    "to": "application_submitted",
                    "value": row["application_submitted"]
                    + row["accepted"]
                    + row["rejected"],
                    "region": row["region"],
                }
            )
            output.append(
                {
                    "from": "application_submitted",
                    "to": "accepted",
                    "value": row["accepted"],
                    "region": row["region"],
                }
            )
            output.append(
                {
                    "from": "application_submitted",
                    "to": "rejected",
                    "value": row["rejected"],
                    "region": row["region"],
                }
            )
        self.df = pd.DataFrame(output, columns=["from", "to", "value", "region"])
        return


######################################
### /end Metric Generating Classes ###
######################################

########################
### Helper Functions ###
########################


class L2Overlay(ABC):
    def __init__(self, MetricClass, digits=2):
        self.mc = MetricClass
        self.params = self.mc.params
        self.digits = digits

    def set(self):
        return self.mc.df


class L2OverlayWeather(L2Overlay):
    def get_weather_data(self):
        sby = self.mc.params.sby
        location_insert = ""
        if self.params.ggie("location") == "":
            location_insert = ", location"
        lby = self.mc.params.by
        if lby == ", location":
            lby = ""
        join_by = ""
        aby = ""
        if sby != "":
            aby = f", a.{sby}"
        if sby in ["day_of_week", "week", "month", "year"]:
            join_by = f" AND a.{sby} = b.{sby}"
        sql = """
        WITH tf_locations AS (
            SELECT  form_id,
                    response_id,
                    TRUNC(date) AS date{location_insert}{full_select_insert}
            FROM {table_name} m{act_join}{h1_join}
            WHERE TRUNC(m.date) >= '{startDate}'
              AND TRUNC(m.date) <= '{endDate}'{job_filter}{filter_wc}
        ), distinct_locations AS (
            SELECT  date, location{lby}
            FROM tf_locations
            GROUP BY date, location{lby}
            ORDER BY date, location{lby}
        ), weather AS (
            SELECT  TRIM(' ' FROM TO_CHAR(dt::DATE, 'Day')) AS day_of_week,
                    TO_CHAR(DATE_TRUNC('week', dt::DATE), 'YYYY-MM-DD') AS week,
                    LEFT(dt::VARCHAR, 7) AS month,
                    LEFT(dt::VARCHAR, 4) AS year,
                    dt AS date,
                    location,
                    min_temp,
                    max_temp,
                    avg_temp,
                    condition
            FROM weather_records
        ), joined AS (
            SELECT  1 AS grouping_col,
                    b.min_temp, b.avg_temp, b.max_temp, b.condition{aby}
            FROM distinct_locations a
            JOIN weather b ON a.date = b.date AND a.location = b.location{join_by}
        ), condition_counts AS (
            SELECT grouping_col, condition, COUNT(condition) AS condition_count{by}
            FROM joined
            GROUP BY grouping_col, condition{by}
        ), rank AS (
            SELECT  grouping_col,
                    DENSE_RANK () OVER ( PARTITION BY grouping_col{by}
                                         ORDER BY condition_count DESC, condition)
                        AS condition_rnk,
                    condition{by}
            FROM condition_counts
        ), rejoined AS (
            SELECT  a.grouping_col,
                    a.min_temp, a.avg_temp, a.max_temp,
                    b.condition{aby}
            FROM joined a
            JOIN rank b
              ON b.condition_rnk = 1 AND a.condition = b.condition{join_by}
        )
        SELECT  MIN(min_temp) AS min_temp,
                AVG(avg_temp) AS avg_temp,
                MAX(max_temp) AS max_temp,
                ANY_VALUE(condition) AS condition{by}
        FROM rejoined
        GROUP BY grouping_col{by}
        ;"""
        # Pulling from TF data; no project_id available.
        # pf = f" AND w.project_id = '{self.params.job}'" if len(self.params.job) > 0 \
        #     else ''
        sql_dict = {
            "location_insert": location_insert,
            "lby": lby,
            "aby": aby,
            "join_by": join_by,
        }
        sql = self.mc.fill_sql_inserts(sql, sql_dict)
        df = self.mc.make_df(sql, to_self=False)
        return df

    def attach_weather_data(self, df):
        mdf = self.mc.df
        # print(mdf)
        # print(df)
        if df.empty:
            return mdf
        if self.params.sby != "":
            mdf = mdf.merge(df, on=self.params.sby, how="left")
        else:
            if mdf.index.size == 1:  # If there are any values
                mdf_values = mdf.iloc[0]  # Get the values of each column in mdf
                # Create columns in df based on mdf columns and assign mdf values
                for column in mdf.columns:
                    df[column] = mdf_values[column]
            mdf = df
        # print(mdf)
        return mdf

    def set(self):
        weather_groupings = [
            "region",
            "location",
            "hierarchy2",
            "day_of_week",
            "week",
            "month",
            "year",
        ]
        gd = self.params.groupings_dict.copy()
        gd = [key for key in gd if len(gd[key]["select"]) > 0]
        fd = self.params.filters_dict.copy()
        fd = [key for key in fd if len(fd[key]["select"]) > 0]
        if len(fd) > 0:
            for key in fd:
                if key not in weather_groupings:
                    print(f"{key} not a weather-grouping.")
                    return self.mc.df
        if len(gd) == 0:
            print("No grouping.")
        elif gd[0] not in weather_groupings:
            print(f"Wrong kind of grouping: {gd[0]}")
            return self.mc.df
        df = self.get_weather_data()
        df = self.attach_weather_data(df)
        for i in ["min_temp", "avg_temp", "max_temp", "condition"]:
            if i in df.columns:
                df[i] = df[i].fillna("n/a")
        return df


class L1Getter(ABC):
    def __init__(self, MetricClass, digits=1, metric_type_suffix=""):
        self.metric = MetricClass
        self.digits = digits
        self.metric_type_suffix = metric_type_suffix

    @abstractmethod
    def get_l1(self):
        pass


class L1DateGetter(L1Getter):
    def get_l1(self):
        # self.metric.setL1()
        self.metric.startDate = datetime.utcnow().strftime("%Y-%m-%d")
        self.metric.endDate = self.metric.startDate
        self.metric.return_dict = self.metric.df.to_dict(orient="list")


class L1DateGetterRecords(L1Getter):
    def get_l1(self):
        # self.metric.setL1()
        self.metric.startDate = datetime.utcnow().strftime("%Y-%m-%d")
        self.metric.endDate = self.metric.startDate
        m = self.metric.params.metric
        self.metric.df["unrounded"] = self.metric.params.metric
        self.metric.df[m] = self.metric.df[m].apply(human_format, dec=self.digits)
        self.metric.return_dict = self.metric.df.to_dict(orient="records")


class L1IndicatorGetter(L1Getter):
    def get_l1(self) -> None:
        if len(self.metric.df.index) > 0:
            job_metric_dict = self.metric.job_metric_dict
            metric_dict = self.metric.metric_dict
            params = self.metric.params
            job = params.job
            metric = params.metric

            if job in job_metric_dict and metric in job_metric_dict[job]:
                metric_config = job_metric_dict[job][metric]

                if "indicator_class" in metric_config:
                    below_target_good = False
                    if "btg" in metric_dict[metric]:
                        below_target_good = metric_dict[metric]["btg"]
                    indicator_class = metric_config["indicator_class"](
                        self.metric.df, params, below_target_good
                    )
                    self.metric.df = indicator_class.applyIndicator(
                        self.metric.engine, metric_dict, job_metric_dict
                    )

                if "num_to_percent" in metric_config:
                    num_cols = metric_config["num_to_percent"]
                    print(self.metric.df)
                    for col in num_cols:
                        if isinstance(self.metric.df[col][0], Number):
                            ser = self.metric.df[col]
                            ser_nn = ser[ser.notnull()]
                            self.metric.df.loc[ser_nn.index, col] = (
                                ser_nn.astype(str) + "%"
                            )

            self.metric.df = self.metric.df.fillna("Blank")
            self.metric.df["unrounded"] = self.metric.df[metric]
            self.metric.df[metric] = self.metric.df[metric].apply(
                human_format, dec=self.digits
            )
            if self.metric.df[metric][0][-2:] == ".0":
                self.metric.df[metric][0] = self.metric.df[metric][0].replace(".0", "")
        # Add prefixes (currency) or suffixes ("%", " min.") to L1.
        self.add_ixes_to_l1()
        self.metric.return_dict = self.metric.df.to_dict(orient="records")

    def add_ixes_to_l1(self) -> None:
        """ix --> prefix or suffix"""
        jmd = self.metric.job_metric_dict
        metric = self.metric.params.metric
        if "job" in self.metric.params.ai:
            job = self.metric.params.job
            if job in jmd and metric in jmd[job]:
                pre = ""
                suf = (
                    self.metric_type_suffix
                )  # replace with job-level suffix if exists.
                if "prefix" in jmd[job][metric]:
                    pre = jmd[job][metric]["prefix"]
                if suf in jmd[job][metric]:
                    suf = jmd[job][metric]["suffix"]
                self.metric.df[metric] = pre + self.metric.df[metric].astype(str) + suf
                self.metric.return_dict = self.metric.df.to_dict(orient="records")


class S3JsonBucket:
    def __init__(self, bucket_name):
        s3 = boto3.resource("s3", **aws_creds)
        self.bucket = s3.Bucket(bucket_name)

    def load(self, key):
        return json.load(self.bucket.Object(key=key).get()["Body"])

    def dump(self, key, obj, use_json=True):
        exp = datetime.now() + timedelta(minutes=2)
        obj = json.dumps(obj) if use_json else obj
        dumped = self.bucket.Object(key=key).put(Body=obj, Expires=exp)
        return dumped


def createPresignedUrl(object_name, bucket_name=bucket, expiration=60):
    """Generate a presigned URL to share an S3 object
    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """
    # Generate a presigned URL for the S3 object
    s3_client = boto3.client("s3", **aws_creds)
    try:
        response = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=expiration,
        )
    except ClientError as e:
        logging.error(e)
        return None
    # The response contains the presigned URL
    return response


def getValueForEveryGrouping(
    df, col, by, target, dindex="date", extra_pivots=["size", "frac_lost"]
):
    if by in ["", "all"]:
        df["rnk"] = 1
        return df
    by = by.replace(", ", "")
    pivotted_df = df.pivot(index=dindex, columns=by, values=col).fillna(0).reset_index()
    valvars = list(df[by].unique())
    valvars = [x for x in valvars if x is not None]
    melted_df = pd.melt(
        pivotted_df, id_vars=[dindex], value_vars=valvars, var_name=by, value_name=col
    )
    for x in extra_pivots:
        if x in df.columns:
            pivotted_x_df = (
                df.pivot(index=dindex, columns=by, values=x).fillna(0).reset_index()
            )
            melted_x_df = pd.melt(
                pivotted_x_df,
                id_vars=[dindex],
                value_vars=valvars,
                var_name=by,
                value_name=x,
            )
            melted_df = pd.merge(melted_df, melted_x_df, how="inner", on=[by, dindex])
    rejoin_list = [by, "rnk", "region_target", "target"]
    if target == "":
        rejoin_list = [by, "rnk"]
    rejoined_df = pd.merge(
        melted_df, df[rejoin_list].drop_duplicates(), how="inner", on=by
    )
    # Omitting rdc code; irrelevant?
    rejoined_df.sort_values([dindex, "rnk"], inplace=True)
    return rejoined_df


#############################
### /end Helper Functions ###
#############################
