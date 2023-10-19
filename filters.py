from abc import ABC, abstractmethod
import pandas as pd
from copy import deepcopy

from metric_generators import MetricFromParameters
from helpers import jnInserter, findNth, valid_hierarchies

######################
### Filter Options ###
######################


class FindFilters(ABC):
    __slots__ = ["engine", "metric_object"]

    def __init__(self, engine, metric_object: MetricFromParameters):
        self.engine = engine
        self.metric_object = metric_object
        self.setValidGroupings()
        # Reset as part of FindFilters class initialization because
        # we need access to FF' .setValidGroupings() method to reliably
        # determine which filters are valid.
        self.metric_object.params.omitInvalidFilters(self.metric_object.valid_groupings)

    @abstractmethod
    def setValidGroupings(self):
        pass

    def setValidFilters(self, filter_method_dict):
        # Example filter_method_dict:
        # {'date':self.setFilteredDates, 'week':self.setWeekOptions,
        #  'day_of_week':self.setDayOfWeekOptions, 'location':self.setLocationOptions}
        self.setValidGroupings()
        self.metric_object.filters = {}
        specific_filter = self.metric_object.params.filter
        if specific_filter is None:
            [filter_method_dict[mthd]() for mthd in filter_method_dict.keys()]
            rd = {}
            # print(self.metric_object.filters.keys())
            for key in filter_method_dict.keys():
                rd[key] = self.metric_object.filters[key]
        else:
            # print("setValidFilters()' filter_method_dict[specific_filter]")
            # print(filter_method_dict[specific_filter])
            filter_method_dict[specific_filter]()
            rd = {specific_filter: self.metric_object.filters[specific_filter]}
        self.metric_object.return_dict = rd

    def setFilteredDates(self):
        """Based on MetricTF's .setAllDates(); different because need filters."""
        sql = self.getFilteredDatesSQL()
        # print(sql)
        dts = pd.read_sql(sql, self.engine)
        dts = list(dts["date"])
        dts = [x for x in dts if x is not None]
        # print("dts")
        # print(dts)
        dr = pd.date_range(dts[0], dts[-1])
        da = dr.isin(dts)  # date array
        da = [int(i) for i in da]
        self.metric_object.filters["date"] = {
            "startDate": str(dts[0]),
            "endDate": str(dts[-1]),
            "dateArray": da,
        }

    @abstractmethod
    def getValidFilters(self):
        pass


class FiltersBlank(FindFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {}
        return  # No groupings needed.

    def getValidFilters(self):
        self.setValidFilters({})


class FindTableName(FindFilters):
    """Set table_name: base_lost_x <-- lost; base_pnr_x <-- pnr"""

    __slots__ = ["table_name"]

    def __init__(self, engine, metric_object: MetricFromParameters):
        self.setTableName()
        super(FindTableName, self).__init__(engine, metric_object)

    @abstractmethod
    def setTableName(self):
        pass


class FindTFFormatFilters(FindTableName):
    __slots__ = []

    def getFilteredDatesSQL(self, get_count=False):
        sql = """
        SELECT {oc}DISTINCT m.date::DATE{cc} AS date
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        LEFT JOIN m.act AS activity_type ON TRUE
        """
        sql_suffix = """
        WHERE m.field_ref LIKE '{field_ref}%%'{job_filter}{filter_wc}
        ORDER BY date
        ;"""
        if self.metric_object.params.field_ref is None:
            sql_suffix = """
        WHERE TRUE{job_filter}{filter_wc}
        ORDER BY date
        ;"""
        sql = sql + sql_suffix
        oc = ""
        cc = ""
        if get_count:
            oc = "COUNT("
            cc = ")"
        sql = sql.format(
            oc=oc,
            cc=cc,
            filter_wc=self.metric_object.params.filters_string,
            job_filter=self.metric_object.params.job_filter,
            table_name=self.table_name,
            field_ref=self.metric_object.params.field_ref,
        )
        # print(sql)
        return sql

    def setSimpleOption(self, opt_str):
        where_use_metric = f"m.field_ref LIKE '{self.metric_object.params.field_ref}%%'"
        if self.metric_object.params.field_ref is None:
            where_use_metric = "TRUE"
        sql = """
        SELECT  DISTINCT {opt_str}::VARCHAR
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        LEFT JOIN m.act AS activity_type ON TRUE
        WHERE {where_use_metric}
          AND TRUNC(date) >= '{startDate}'
          AND TRUNC(date) <= '{endDate}'
          AND {opt_str} IS NOT NULL{job_filter}{filter_wc}
        ORDER BY {opt_str}::VARCHAR
        ;"""
        sql = sql.format(
            opt_str=opt_str,
            where_use_metric=where_use_metric,
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            job_filter=self.metric_object.params.job_filter,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[opt_str])
        self.metric_object.filters[opt_str] = fList

    def setWeekOptions(self):
        self.setSimpleOption("week")

    def setMonthOptions(self):
        self.setSimpleOption("month")

    def setYearOptions(self):
        self.setSimpleOption("year")

    def setGenderOptions(self):
        self.setSimpleOption("gender")

    def setSexOptions(self):
        self.setSimpleOption("sex")

    def setFinancialFirmOptions(self):
        self.setSimpleOption("financial_firm")

    def setAgeGroupOptions(self):
        self.setSimpleOption("age_group")

    def setTeamOptions(self):
        self.setSimpleOption("team")

    def setDayOfWeekOptions(self):
        self.setSimpleOption("day_of_week")
        fList = self.metric_object.filters["day_of_week"]
        dowOrder = []
        [dowOrder.append(fList.index(i)) for i in day_order_dict.keys() if i in fList]
        fList = [fList[i] for i in dowOrder]
        self.metric_object.filters["day_of_week"] = fList

    def setGenericOptions(self, filterby):
        where_use_metric = f"m.field_ref LIKE '{self.metric_object.params.field_ref}%%'"
        if self.metric_object.params.field_ref is None:
            where_use_metric = "TRUE"
        vh_at_hx = filterby
        if filterby in valid_hierarchies:
            vh_at_hx = valid_hierarchies[filterby]
        sql = """
        SELECT DISTINCT REPLACE({filterby}, CHR(37), 'pc.') AS {filterby}
        FROM (  SELECT  {vh_at_hx}::VARCHAR AS {filterby}
                FROM {table_name} m
                LEFT JOIN m.pt AS product_type ON TRUE
                LEFT JOIN m.act AS activity_type ON TRUE
                WHERE {where_use_metric}
                    AND TRUNC(date) >= '{startDate}'
                    AND TRUNC(date) <= '{endDate}'{job_filter}{filter_wc}) fb
        WHERE {filterby} IS NOT NULL
        ORDER BY {filterby}
        ;"""
        sql = sql.format(
            filterby=filterby,
            vh_at_hx=vh_at_hx,
            where_use_metric=where_use_metric,
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            job_filter=self.metric_object.params.job_filter,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[filterby])
        # Replace ' characters.
        fList = [s.replace("'", "\\'") if isinstance(s, str) else s for s in fList]
        self.metric_object.filters[filterby] = fList

    def setHxOptions(self, x):
        """Get all the hierarchy-x options."""
        self.setGenericOptions(f"hierarchy{x}")
        hh = HierarchicalHelpers(self.engine, self.metric_object)
        hh.parentDictFromNodeIds(
            self.metric_object.filters[f"hierarchy{x}"], f"hierarchy{x}"
        )

    def setH1Options(self):
        """Get hierarchy1 / product_type options."""
        self.setHxOptions(1)

    def setH2Options(self):
        """Get hierarchy2 / location options."""
        self.setHxOptions(2)

    def setH7Options(self):
        """Get hierarchy7 / project_id options."""
        self.setHxOptions(7)

    def setLocationOptions(self):
        self.setGenericOptions("location")


class FiltersTF(FindTFFormatFilters):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class FiltersTFDowWeekTeam(FiltersTF):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "team": {"label": "Team"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "team": self.setTeamOptions,
            }
        )


class FiltersTFDoWWeek(FiltersTF):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
            }
        )


class FiltersTFDowWeekLocation(FiltersTF):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "location": {"label": "Location"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "location": self.setLocationOptions,
            }
        )


class FiltersTFDowWeekLocationAgeSex(FiltersTF):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "location": {"label": "Location"},
            "age_group": {"label": "Age Group"},
            "sex": {"label": "Gender"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "location": self.setLocationOptions,
                "age_group": self.setAgeGroupOptions,
                "sex": self.setSexOptions,
            }
        )

    def setTableName(self):
        self.table_name = "tf_queries"


class FiltersTFDowWeekLocationSexFinancialFirm(FiltersTF):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "location": {"label": "Location"},
            "sex": {"label": "Gender"},
            "financial_firm": {"label": "Financial Firm"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "location": self.setLocationOptions,
                "sex": self.setSexOptions,
                "financial_firm": self.setFinancialFirmOptions,
            }
        )

    def setTableName(self):
        self.table_name = "tf_queries"


class FiltersTFNoGrouping(FiltersTF):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {}

    def getValidFilters(self):
        self.setValidFilters({"date": self.setFilteredDates})


class HierarchicalHelpers:
    __slots__ = ["engine", "metric_object"]

    def __init__(self, engine, metric_object: MetricFromParameters):
        self.engine = engine
        self.metric_object = metric_object

    def getHXAncestry(self, node_names, hierarchyx):
        """node_names is a list of node names
        x is the hierarchy_id."""
        sql = f"""
        SELECT node_id, ancestry
        FROM hierarchical_groupings
        WHERE hierarchy_id = {hierarchyx[-1]}
          AND node_name IN ({jnInserter(node_names, dropQ=True, dropNull=True)})
        ORDER BY node_id
        ;"""
        df = pd.read_sql(sql, self.engine)
        return df

    def parentDictFromNodeIds(self, node_names, hierarchyx):
        """Get one ancestry from all of the nodes that were not filtered out."""
        df = self.getHXAncestry(node_names, hierarchyx)
        dctArray = []
        for i in df["ancestry"]:
            dctArray.append(eval(i))
            # print(i)
        for i, d in enumerate(dctArray):
            if i > 0:
                dctArray[0] = self.dictCopier(dctArray[0], dctArray[i])
        if len(dctArray) > 0:
            dct = dctArray[0]
        else:
            dct = ""
        # dct = reduce(self.dictCopier, dctArray)
        self.metric_object.filters[hierarchyx] = dct
        return dct

    def rcrsv_chck(self, d1, d2, key):  # Recursive key check
        range_list_1 = list(range(1, len(d1[key])))
        append_d2_to_d1 = True
        if len(d2[key]) > 1:
            for i in range_list_1:
                if d1[key][i].keys() == d2[key][1].keys():
                    append_d2_to_d1 = False
                    for key_deep in d1[key][i].keys():
                        d1[key][i] = self.rcrsv_chck(d1[key][i], d2[key][1], key_deep)
            if append_d2_to_d1 is True:
                d1[key].append(d2[key][1])
        return d1

    def dictCopier(self, d1, d2):
        d1_keys = list(d1.keys())
        append_d2_to_d1 = True
        for key in d1_keys:
            if key in d2:
                d1 = self.rcrsv_chck(d1, d2, key)
                append_d2_to_d1 = False
        if append_d2_to_d1:
            for k2 in d2:
                d1[k2] = d2[k2]
        return d1


class FiltersTFDowWeekH1H2(FiltersTF):
    # Not sure it makes sense to store these here.  Revisit this when less sleepy.
    __slots__ = ["valid_h1", "select_h1"]
    # Here, we're figuring out how to find the valid hierarchy levels a user can use.

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy1": {"label": "Product Type"},
            "hierarchy2": {"label": "Location"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy1": self.setH1Options,
                "hierarchy2": self.setH2Options,
            }
        )


class FindHierarchicalTFDemoFilters(FiltersTFDowWeekH1H2):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class FindHierarchicalGATFFormatFilters(FiltersTFDowWeekH1H2):
    ### Not sure it makes sense to store these here.  Revisit this when less sleepy.
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "age_group": {"label": "Age Group"},
            "gender": {"label": "Gender"},
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy1": {"label": "Product Type"},
            "hierarchy2": {"label": "Location"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "age_group": self.setAgeGroupOptions,
                "gender": self.setGenderOptions,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy1": self.setH1Options,
                "hierarchy2": self.setH2Options,
            }
        )


class FindHierarchicalGATFFilters(FindHierarchicalGATFFormatFilters):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_queries"


class FindHierarchicalGATFDemoFilters(FindHierarchicalGATFFormatFilters):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_queries"


class FiltersTFDowWeekH2(FiltersTF):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy2": {"label": "Location"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy2": self.setH2Options,
            }
        )


class FiltersTFDowWeekH2H7Team(FiltersTF):
    """Test whether project-hierarchy works."""

    __slots__ = []

    def setValidGroupings(self):
        """Get valid groupings."""
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy2": {"label": "Location"},
            "hierarchy7": {"label": "Project"},
            "team": {"label": "Team"},
        }

    def getValidFilters(self):
        """Get valid filters."""
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy2": self.setH2Options,
                "hierarchy7": self.setH7Options,
                "team": self.setTeamOptions,
            }
        )


class FiltersTFDowWeekMonthH2(FiltersTF):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "month": {"label": "Month"},
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy2": {"label": "Location"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "month": self.setMonthOptions,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy2": self.setH2Options,
            }
        )


# class FiltersTFDowWeekMonth(FiltersTF):
#     __slots__ = []
#     def setValidGroupings(self):
#         self.metric_object.valid_groupings = {
#             'month': {'label': 'Month'},
#             'week': {'label': 'Week'},
#             'day_of_week': {'label': 'Day of Week'},
#         }

#     def getValidFilters(self):
#         self.setValidFilters({
#             'date': self.setFilteredDates,
#             'week': self.setWeekOptions,
#             'day_of_week': self.setDayOfWeekOptions,
#             'month': self.setMonthOptions
#         })


class FiltersTFDowWeekH2Ev(FiltersTFDowWeekH2):
    __slots__ = []

    def setEventTypeOptions(self):
        self.setGenericOptions("event_type")

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy2": {"label": "Location"},
            "event_type": {"label": "Event Type"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy2": self.setH2Options,
                "event_type": self.setEventTypeOptions,
            }
        )


class FiltersTFDowWeekH1H2Ev(FiltersTF):
    __slots__ = []

    def setEventTypeOptions(self):
        self.setGenericOptions("event_type")

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy1": {"label": "Product Type"},
            "hierarchy2": {"label": "Location"},
            "event_type": {"label": "Event Type"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy1": self.setH1Options,
                "hierarchy2": self.setH2Options,
                "event_type": self.setEventTypeOptions,
            }
        )


class FiltersTFDowWeekH2Area(FiltersTF):
    __slots__ = []

    def setAreaOptions(self):
        self.setGenericOptions("area")

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy2": {"label": "Location"},
            "area": {"label": "Area"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy2": self.setH2Options,
                "area": self.setAreaOptions,
            }
        )


class FiltersTFDowWeekMonthYearH1H2Act(FiltersTF):
    __slots__ = []

    def setActivityTypeOptions(self):
        self.setSimpleOption("activity_type")

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "year": {"label": "Year"},
            "month": {"label": "Month"},
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy1": {"label": "Product Type"},
            "hierarchy2": {"label": "Location"},
            "activity_type": {"label": "Activity Type"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "month": self.setMonthOptions,
                "year": self.setYearOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy1": self.setH1Options,
                "hierarchy2": self.setH2Options,
                "activity_type": self.setActivityTypeOptions,
            }
        )


class FiltersTFDowWeekMonthYearActH1H2H7(FiltersTF):
    __slots__ = []

    def setActivityTypeOptions(self):
        self.setSimpleOption("activity_type")

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "activity_type": {"label": "Activity Type"},
            "day_of_week": {"label": "Day of Week"},
            "week": {"label": "Week"},
            "month": {"label": "Month"},
            "year": {"label": "Year"},
            "hierarchy1": {"label": "Product Type"},
            "hierarchy2": {"label": "Location"},
            "hierarchy7": {"label": "Project"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "activity_type": self.setActivityTypeOptions,
                "date": self.setFilteredDates,
                "week": self.setWeekOptions,
                "month": self.setMonthOptions,
                "year": self.setYearOptions,
                "day_of_week": self.setDayOfWeekOptions,
                "hierarchy1": self.setH1Options,
                "hierarchy2": self.setH2Options,
                "hierarchy7": self.setH7Options,
            }
        )


class FindVideoProfileRegions(FindFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {"region": {"label": "Region"}}

    def getFilteredDatesSQL(self, get_count=False):
        # Deliberately ignore any other filters for dates; return only month-starts.
        sql = """
        SELECT *
        FROM base_video_all_dates
        ORDER BY date
        ;"""
        # sql=sql.format( filter_wc=self.metric_object.params.filters_string)
        # print(sql)
        return sql

    def setRegionOptions(self):
        self.metric_object.filters["region"] = ["UK", "US"]

    def getValidFilters(self):
        self.setValidFilters(
            {"date": self.setFilteredDates, "region": self.setRegionOptions}
        )


class FindVideoDateOnly(FindVideoProfileRegions):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {}

    def getValidFilters(self):
        self.setValidFilters({"date": self.setFilteredDates})


class FindActiveRegions(FindFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {"region": {"label": "Region"}}

    def getFilteredDatesSQL(self, get_count=False):
        # Deliberately ignore any other filters for dates; return only month-starts.
        sql = """
        SELECT date AS date
        FROM base_active_all_dates
        ORDER BY date
        ;"""
        # sql=sql.format( filter_wc=self.metric_object.params.filters_string)
        # print(sql)
        return sql

    def setRegionOptions(self):
        self.metric_object.filters["region"] = ["UK", "US", "Germany", "Austria"]

    def getValidFilters(self):
        self.setValidFilters(
            {"date": self.setFilteredDates, "region": self.setRegionOptions}
        )


class FindEnabledStaffRegions(FindFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {"region": {"label": "Region"}}

    def getFilteredDatesSQL(self, get_count=False):
        # Deliberately ignore any other filters for dates; return only month-starts.
        sql = """
        SELECT date AS date
        FROM base_staff_all_dates
        ORDER BY date
        ;"""
        # sql=sql.format( filter_wc=self.metric_object.params.filters_string)
        # print(sql)
        return sql

    def setRegionOptions(self):
        self.metric_object.filters["region"] = ["UK", "US", "Germany", "Austria"]

    def getValidFilters(self):
        self.setValidFilters(
            {"date": self.setFilteredDates, "region": self.setRegionOptions}
        )


class FindBaseRegions(FindFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {"region": {"label": "Region"}}

    def getFilteredDatesSQL(self, get_count=False):
        # Deliberately ignore any other filters for dates; return only month-starts.
        sql = f"""
        SELECT *
        FROM base_{self.metric_object.params.metric}_all_dates
        ORDER BY date
        ;"""
        # sql=sql.format( filter_wc=self.metric_object.params.filters_string)
        # print(sql)
        return sql

    def setSimpleOptions(self, filterby):
        sql = f"""
        SELECT DISTINCT {filterby}
        FROM base_{self.metric_object.params.metric}_dates
        ;"""
        df = pd.read_sql(sql, self.engine)
        fList = list(df[filterby])
        self.metric_object.filters[filterby] = sorted(fList)

    def setRegionOptions(self):
        self.setSimpleOptions("region")

    def getValidFilters(self):
        self.setValidFilters(
            {"date": self.setFilteredDates, "region": self.setRegionOptions}
        )


class FindRegionProjectManager(FindTableName):
    """Set table_name: base_lost_x <-- lost; base_pnr_x <-- pnr"""

    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "hierarchy3": {"label": "Region"},
            "hierarchy4": {"label": "Project"},
            "hierarchy5": {"label": "Manager"},
        }

    def getFilteredDatesSQL(self, get_count=False):
        # Deliberately ignore any other filters for dates; return only month-starts.
        sql = f"""
        SELECT *
        FROM base_{self.table_name}_all_dates
        ORDER BY date
        ;"""
        # print(sql)
        return sql

    def setGenericOptions(self, filterby, coalesce="'None'"):
        sql = """
        SELECT DISTINCT COALESCE({sby}, {coalesce}) AS {filterby}{h3}{h4}{h5}
        FROM base_{table_name}_dates
        WHERE date >= '{startDate}'
          AND date <= '{endDate}'{filter_wc}
        ORDER BY {filterby}
        ;"""
        # Drop extra sql that would duplicate the filterby select(/join).
        filters_dict = deepcopy(self.metric_object.params.filters_dict)
        for grouping in filters_dict.keys():
            if grouping == filterby:
                for key in filters_dict[grouping].keys():
                    if key != "select":
                        filters_dict[grouping][key] = ""
                    else:
                        cut_starting = findNth(filters_dict[grouping][key], """,""", 2)
                        # part_1 = filters_dict[grouping][key][:cut_starting-1]
                        filters_dict[grouping][key] = filters_dict[grouping][key][
                            :cut_starting
                        ]
        sby = valid_hierarchies[filterby] if filterby in valid_hierarchies else filterby
        sql = sql.format(
            filterby=filterby,
            sby=sby,
            coalesce=coalesce,
            h3=filters_dict["hierarchy3"]["select"],
            h4=filters_dict["hierarchy4"]["select"],
            h5=filters_dict["hierarchy5"]["select"],
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[filterby])
        # Replace ' characters.
        fList = [s.replace("'", "\\'") if isinstance(s, str) else s for s in fList]
        self.metric_object.filters[filterby] = sorted(fList)

    def setRegionOptions(self):
        self.setGenericOptions("hierarchy3")
        hh = HierarchicalHelpers(self.engine, self.metric_object)
        hh.parentDictFromNodeIds(self.metric_object.filters["hierarchy3"], "hierarchy3")

    def setProjectOptions(self):
        self.setGenericOptions("hierarchy4")
        hh = HierarchicalHelpers(self.engine, self.metric_object)
        hh.parentDictFromNodeIds(self.metric_object.filters["hierarchy4"], "hierarchy4")

    def setManagerOptions(self):
        self.setGenericOptions("hierarchy5")
        hh = HierarchicalHelpers(self.engine, self.metric_object)
        hh.parentDictFromNodeIds(self.metric_object.filters["hierarchy5"], "hierarchy5")

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "hierarchy3": self.setRegionOptions,
                "hierarchy4": self.setProjectOptions,
                "hierarchy5": self.setManagerOptions,
            }
        )


class FiltersBaseLiveLocationDW(FindTableName):
    __slots__ = ["table_name"]

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
        }

    def getFilteredDatesSQL(self):
        sql = """
        SELECT DISTINCT date{week_select}{dow_select}
        FROM {table_name} m
        WHERE TRUE{job_filter}{filter_wc}
        ORDER BY date
        ;"""
        sql = sql.format(
            week_select=self.metric_object.params.gfie("week"),
            dow_select=self.metric_object.params.gfie("day_of_week"),
            table_name=self.table_name,
            metric=self.metric_object.params.metric,
            job_filter=self.metric_object.params.job_filter,
            filter_wc=self.metric_object.params.filters_string,
        )
        print(sql)
        return sql

    def setSimpleOption(self, opt_str):
        sql = """
        SELECT  DISTINCT {opt_str}::VARCHAR
        FROM {table_name} m
        WHERE date >= '{startDate}'
          AND date <= '{endDate}'
          AND {opt_str} IS NOT NULL{job_filter}{filter_wc}
        ORDER BY {opt_str}::VARCHAR
        ;"""
        sql = sql.format(
            opt_str=opt_str,
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            job_filter=self.metric_object.params.job_filter,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[opt_str])
        self.metric_object.filters[opt_str] = fList

    def setWeekOptions(self):
        self.setSimpleOption("week")

    def setDayOfWeekOptions(self):
        self.setSimpleOption("day_of_week")
        fList = self.metric_object.filters["day_of_week"]
        dowOrder = []
        [dowOrder.append(fList.index(i)) for i in day_order_dict.keys() if i in fList]
        fList = [fList[i] for i in dowOrder]
        self.metric_object.filters["day_of_week"] = fList

    def setTableName(self):
        self.table_name = "base_live_locations"

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "day_of_week": self.setDayOfWeekOptions,
                "week": self.setWeekOptions,
            }
        )


class FindPNRRegionProjectManager(FindRegionProjectManager):
    """Set table_name: base_pnr_x <-- pnr"""

    __slots__ = []

    def setTableName(self):
        self.table_name = "pnr"


class FindLostRegionProjectManager(FindRegionProjectManager):
    """Set table_name: base_lost_x <-- lost"""

    __slots__ = []

    def setTableName(self):
        self.table_name = "lost"


class FindLostRegionProjectManagerUnapproved(FindLostRegionProjectManager):
    """Set table_name: base_lost_x <-- lost"""

    __slots__ = []

    def setPaidUnapprovedOptions(self):
        self.setGenericOptions("paid_unapproved", 0)

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "hierarchy3": {"label": "Region"},
            "hierarchy4": {"label": "Project"},
            "hierarchy5": {"label": "Manager"},
            "paid_unapproved": {"label": "Paid Unapproved"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "hierarchy3": self.setRegionOptions,
                "hierarchy4": self.setProjectOptions,
                "hierarchy5": self.setManagerOptions,
                "paid_unapproved": self.setPaidUnapprovedOptions,
            }
        )


class FindClientNPS(FindFilters):
    __slots__ = []

    ### WRITE THIS
    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "region": {"label": "Region"},
            "hierarchy6": {"label": "Client"},
        }

    def getFilteredDatesSQL(self, get_count=False):
        sql = """
        SELECT DISTINCT date AS date{region_select}{h6_select}
        FROM base_client_nps_dates m{h6_join}
        WHERE TRUE{filter_wc}
        ORDER BY date
        """
        sql = sql.format(
            region_select=self.metric_object.params.gfie("region"),
            h6_select=self.metric_object.params.gfie("hierarchy6"),
            h6_join=self.metric_object.params.gfie("hierarchy6", "join"),
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        return sql

    def setGenericOptions(self, filterby):
        sql = """
        SELECT DISTINCT COALESCE({sby}, 'None') AS {filterby}{h6}
        FROM base_client_nps_dates
        WHERE date >= '{startDate}'
          AND date <= '{endDate}'{filter_wc}
        ORDER BY {filterby} 
        ;"""
        # Drop extra sql that would duplicate the filterby select(/join).
        filters_dict = deepcopy(self.metric_object.params.filters_dict)
        for grouping in filters_dict.keys():
            if grouping == filterby:
                for key in filters_dict[grouping].keys():
                    if key != "select":
                        filters_dict[grouping][key] = ""
                    else:
                        cut_starting = findNth(filters_dict[grouping][key], """,""", 2)
                        # part_1 = filters_dict[grouping][key][:cut_starting-1]
                        filters_dict[grouping][key] = filters_dict[grouping][key][
                            :cut_starting
                        ]
        sql = sql.format(
            filterby=filterby,
            sby=valid_hierarchies[filterby],
            h6=filters_dict["hierarchy6"]["select"],
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[filterby])
        # Replace ' characters.
        fList = [s.replace("'", "\\'") if isinstance(s, str) else s for s in fList]
        self.metric_object.filters[filterby] = fList

    def setRegionOptions(self):
        self.metric_object.filters["region"] = ["UK", "US", "Germany", "France"]

    def setClientOptions(self):
        self.setGenericOptions("hierarchy6")
        hh = HierarchicalHelpers(self.engine, self.metric_object)
        hh.parentDictFromNodeIds(self.metric_object.filters["hierarchy6"], "hierarchy6")

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "region": self.setRegionOptions,
                "hierarchy6": self.setClientOptions,
            }
        )


class FindDate(FindFilters):
    __slots__ = []

    ### WRITE THIS
    def setValidGroupings(self):
        self.metric_object.valid_groupings = {}

    def getFilteredDatesSQL(self, get_count=False):
        sql = """
        SELECT DISTINCT date AS date
        FROM base_{metric}_dates
        ORDER BY date
        """
        sql = sql.format(metric=self.metric_object.params.metric)
        # print(sql)
        return sql

    def getValidFilters(self):
        self.setValidFilters({"date": self.setFilteredDates})


class FindComplianceDashDate(FindFilters):
    __slots__ = []

    def getFilteredDatesSQL(self):
        sql = """
        SELECT DISTINCT date{year_select}{month_select}{week_select}{dow_select}
        FROM base_{metric}_dates m
        WHERE TRUE{filter_wc}
        ORDER BY date
        ;"""
        sql = sql.format(
            year_select=self.metric_object.params.gfie("year"),
            month_select=self.metric_object.params.gfie("month"),
            week_select=self.metric_object.params.gfie("week"),
            dow_select=self.metric_object.params.gfie("day_of_week"),
            metric=self.metric_object.params.metric,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        return sql

    def setWeekOptions(self):
        sql = """
        SELECT  DISTINCT DATE_TRUNC('week', TO_TIMESTAMP(date, 'YYYY-MM-DD')) AS week
        FROM (  SELECT  TO_CHAR(date, 'YYYY-MM-DD') AS date{year}{month}{dow}
                FROM base_{metric}_dates m
                WHERE date >= '{startDate}'
                  AND date <= '{endDate}'{filter_wc})
        ORDER BY week
        ;"""
        sql = sql.format(
            metric=self.metric_object.params.metric,
            year=self.metric_object.params.gfie("year"),
            month=self.metric_object.params.gfie("month"),
            dow=self.metric_object.params.gfie("day_of_week"),
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df["week"])
        fList = [i.strftime("%Y-%m-%d") for i in fList]
        self.metric_object.filters["week"] = fList

    def setMonthOptions(self):
        sql = """
        SELECT  DISTINCT LEFT(date, 7) AS month
        FROM (  SELECT  TO_CHAR(date, 'YYYY-MM-DD') AS date{year}{week}{dow}
                FROM base_{metric}_dates m
                WHERE date >= '{startDate}'
                  AND date <= '{endDate}'{filter_wc})
        ORDER BY month
        ;"""
        sql = sql.format(
            metric=self.metric_object.params.metric,
            year=self.metric_object.params.gfie("year"),
            week=self.metric_object.params.gfie("week"),
            dow=self.metric_object.params.gfie("day_of_week"),
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df["month"])
        self.metric_object.filters["month"] = fList

    def setYearOptions(self):
        sql = """
        SELECT  DISTINCT LEFT(date, 4) AS year
        FROM (  SELECT  TO_CHAR(date, 'YYYY-MM-DD') AS date{month}{week}{dow}
                FROM base_{metric}_dates m
                WHERE date >= '{startDate}'
                  AND date <= '{endDate}'{filter_wc})
        ORDER BY year
        ;"""
        sql = sql.format(
            metric=self.metric_object.params.metric,
            month=self.metric_object.params.gfie("month"),
            week=self.metric_object.params.gfie("week"),
            dow=self.metric_object.params.gfie("day_of_week"),
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df["year"])
        self.metric_object.filters["year"] = fList

    def setDayOfWeekOptions(self):
        sql = """
        SELECT  DISTINCT TRIM(' ' FROM TO_CHAR(TO_TIMESTAMP(date, 'YYYY-MM-DD'), 'Day'))
                    AS day_of_week
        FROM (  SELECT  TO_CHAR(date, 'YYYY-MM-DD') AS date{year}{month}{week}
                FROM base_{metric}_dates m
                WHERE date >= '{startDate}'
                  AND date <= '{endDate}'{filter_wc}) 
        ;"""
        sql = sql.format(
            metric=self.metric_object.params.metric,
            year=self.metric_object.params.gfie("year"),
            month=self.metric_object.params.gfie("month"),
            week=self.metric_object.params.gfie("week"),
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df["day_of_week"])
        dowOrder = []
        [dowOrder.append(fList.index(i)) for i in day_order_dict.keys() if i in fList]
        fList = [fList[i] for i in dowOrder]
        self.metric_object.filters["day_of_week"] = fList

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "year": {"label": "Year"},
            "month": {"label": "Month"},
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "year": self.setYearOptions,
                "month": self.setMonthOptions,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
            }
        )


class FindComplianceDash(FindComplianceDashDate):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "activity_type": {"label": "Activity Type"},
            "year": {"label": "Year"},
            "month": {"label": "Month"},
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy1": {"label": "Product Type"},
            "hierarchy2": {"label": "Location"},
        }

    def getFilteredDatesSQL(self):
        sql = """
        SELECT *{year_select}{month_select}{week_select}{dow_select}
        FROM base_{metric}_dates
        WHERE TRUE{filter_wc}
        ORDER BY date
        ;"""
        # print(sql)
        sql = sql.format(
            year_select=self.metric_object.params.gfie("year"),
            month_select=self.metric_object.params.gfie("month"),
            week_select=self.metric_object.params.gfie("week"),
            dow_select=self.metric_object.params.gfie("day_of_week"),
            filter_wc=self.metric_object.params.filters_string,
            metric=self.metric_object.params.metric,
        )
        return sql

    def setGenericOptions(self, filterby):
        replace_filterby = filterby
        if filterby[:9] == "hierarchy":
            replace_filterby = f"{valid_hierarchies[filterby]} AS {filterby}"
        sql = """
        SELECT DISTINCT {replace_filterby}{year}{month}{week}{dow}
        FROM base_{metric}_dates
        WHERE date >= '{startDate}'
          AND date <= '{endDate}'{filter_wc}
        ORDER BY {filterby} 
        ;"""
        # Drop extra sql that would duplicate the filterby select(/join).
        filters_dict = deepcopy(self.metric_object.params.filters_dict)
        for grouping in filters_dict.keys():
            if grouping == filterby:
                for key in filters_dict[grouping].keys():
                    if key != "select":
                        filters_dict[grouping][key] = ""
                    else:
                        cut_starting = findNth(filters_dict[grouping][key], """,""", 2)
                        # part_1 = filters_dict[grouping][key][:cut_starting-1]
                        filters_dict[grouping][key] = filters_dict[grouping][key][
                            :cut_starting
                        ]
        sql = sql.format(
            filterby=filterby,
            replace_filterby=replace_filterby,
            year=self.metric_object.params.gfie("year"),
            month=self.metric_object.params.gfie("month"),
            week=self.metric_object.params.gfie("week"),
            dow=self.metric_object.params.gfie("day_of_week"),
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            metric=self.metric_object.params.metric,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[filterby])
        # Replace ' characters.
        fList = [s.replace("'", "\\'") if isinstance(s, str) else s for s in fList]
        fList = [i for i in fList if i is not None]
        self.metric_object.filters[filterby] = fList

    def setHxOptions(self, x):
        """Get all the hierarchy-x options."""
        self.setGenericOptions(f"hierarchy{x}")
        hh = HierarchicalHelpers(self.engine, self.metric_object)
        hh.parentDictFromNodeIds(
            self.metric_object.filters[f"hierarchy{x}"], f"hierarchy{x}"
        )

    def setH1Options(self):
        """Get hierarchy1 / product_type options."""
        self.setHxOptions(1)

    def setH2Options(self):
        """Get hierarchy2 / location options."""
        self.setHxOptions(2)

    def setActivityTypeOptions(self):
        self.setGenericOptions("activity_type")

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "hierarchy1": self.setH1Options,
                "hierarchy2": self.setH2Options,
                "activity_type": self.setActivityTypeOptions,
                "year": self.setYearOptions,
                "month": self.setMonthOptions,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
            }
        )


class FindSimpleProjectLess(FindTableName):
    """Set table_name; See FindTableName."""

    __slots__ = []

    @abstractmethod
    def setTableName(self):
        pass

    @abstractmethod
    def setValidGroupings(self):
        pass

    def get_filtered_dates_format(self, string_format=True):
        """Date is a string."""
        str_insert = ""
        if string_format is True:
            str_insert = """
          AND date !=''"""
        sql = f"""
        SELECT DISTINCT date
        FROM {self.table_name}
        WHERE date IS NOT NULL{str_insert}{self.metric_object.params.filters_string}
        ORDER BY date
        ;"""
        print(sql)
        return sql

    def getFilteredDatesSQL(self):
        return self.get_filtered_dates_format(string_format=True)

    def setGenericOptions(self, filterby):
        sql = """
        SELECT DISTINCT COALESCE({filterby}, 'None') AS {filterby}
        FROM {table_name}
        WHERE date >= '{startDate}'
          AND date <= '{endDate}'{filter_wc}
        ORDER BY {filterby} 
        ;"""
        # Drop extra sql that would duplicate the filterby select(/join).
        filters_dict = deepcopy(self.metric_object.params.filters_dict)
        for grouping in filters_dict.keys():
            if grouping == filterby:
                for key in filters_dict[grouping].keys():
                    if key != "select":
                        filters_dict[grouping][key] = ""
        sql = sql.format(
            filterby=filterby,
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[filterby])
        # Replace ' characters.
        fList = [s.replace("'", "\\'") if isinstance(s, str) else s for s in fList]
        self.metric_object.filters[filterby] = fList

    def setRegionOptions(self):
        self.setGenericOptions("region")

    def setMonthOptions(self):
        self.setGenericOptions("month")

    def setYearOptions(self):
        self.setGenericOptions("year")

    def setFuelOptions(self):
        self.setGenericOptions("fuel")

    def setQuarterOptions(self):
        self.setGenericOptions("quarter")

    def setClientOptions(self):
        self.setGenericOptions("client")

    def set_day_of_week_options(self):
        self.setGenericOptions("day_of_week")

    def set_week_options(self):
        self.setGenericOptions("week")

    def set_location_options(self):
        self.setGenericOptions("location")

    @abstractmethod
    def getValidFilters(self):
        pass


class FindSimpleDayofWeekWeek(FindSimpleProjectLess):
    __slots__ = []

    @abstractmethod
    def setTableName(self):
        pass

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "day_of_week": {"label": "Day of Week"},
            "week": {"label": "Week"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "day_of_week": self.set_day_of_week_options,
                "week": self.set_week_options,
            }
        )


class FindSimpleYearMonthRegion(FindSimpleProjectLess):
    """Must set table_name; See FindSimple."""

    __slots__ = []

    @abstractmethod
    def setTableName(self):
        pass

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "region": {"label": "Region"},
            "month": {"label": "Month"},
            "year": {"label": "Year"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "region": self.setRegionOptions,
                "month": self.setMonthOptions,
                "year": self.setYearOptions,
            }
        )


class FiltersXfactrDowWeek(FindSimpleDayofWeekWeek):
    def setTableName(self):
        self.table_name = "stores_activated_xfctr"


class FindSFStatus(FindSimpleYearMonthRegion):
    __slots__ = []

    def setTableName(self):
        self.table_name = "sf_impact_project_status_report"


class FindSFRevenue(FindSimpleYearMonthRegion):
    __slots__ = []

    def setTableName(self):
        self.table_name = "sf_impact_api_integration_report_2"


class FindSimpleYearRegion(FindSimpleProjectLess):
    """Must set table_name; See FindSimple."""

    __slots__ = []

    @abstractmethod
    def setTableName(self):
        pass

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "year": {"label": "Year"},
            "region": {"label": "Region"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "year": self.setYearOptions,
                "region": self.setRegionOptions,
            }
        )


class FiltersSimpleDayOfWeekWeekLocation(FindSimpleProjectLess):
    """Must set table_name; See FindSimple."""

    __slots__ = []

    @abstractmethod
    def setTableName(self):
        pass

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "day_of_week": {"label": "Day of Week"},
            "week": {"label": "Week"},
            "location": {"label": "Location"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "day_of_week": self.set_day_of_week_options,
                "week": self.set_week_options,
                "location": self.set_location_options,
            }
        )


class FindSusCompliance(FindSimpleYearMonthRegion):
    __slots__ = []

    def setTableName(self):
        self.table_name = "sus_field_staff_compliance_dates"


class FindSusFieldStaff(FindSimpleYearRegion):
    __slots__ = []

    def setTableName(self):
        self.table_name = "sus_field_staff_sustainability_dates"


class FindEmpMOT(FindSimpleYearRegion):
    __slots__ = []

    def getFilteredDatesSQL(self):
        return self.get_filtered_dates_format(string_format=False)

    def setTableName(self):
        self.table_name = "sus_employee_mot"


class FindEmployeeDistance(FindSimpleYearRegion):
    __slots__ = []

    def setTableName(self):
        self.table_name = "sus_employee_travel_distances"


class FindBusMOT(FindSimpleYearRegion):
    __slots__ = []

    def getFilteredDatesSQL(self):
        return self.get_filtered_dates_format(string_format=False)

    def setTableName(self):
        self.table_name = "sus_business_mot"


class FindBusDistance(FindSimpleYearRegion):
    __slots__ = []

    def getFilteredDatesSQL(self):
        return self.get_filtered_dates_format(string_format=False)

    def setTableName(self):
        self.table_name = "sus_business_distances"


class FindTreesPlantedFilters(FindSimpleProjectLess):
    __slots__ = []

    def getFilteredDatesSQL(self):
        return self.get_filtered_dates_format(string_format=False)

    def setTableName(self):
        self.table_name = "sus_trees_planted"

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "year": {"label": "Year"},
            "client": {"label": "Client"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "year": self.setYearOptions,
                "client": self.setClientOptions,
            }
        )


class FindEnergyUse(FindSimpleProjectLess):
    __slots__ = []

    def getFilteredDatesSQL(self):
        return self.get_filtered_dates_format(string_format=False)

    def setTableName(self):
        self.table_name = "sus_gas_electric_tracker"

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "year": {"label": "Year"},
            "region": {"label": "Region"},
            "fuel": {"label": "Fuel"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "year": self.setYearOptions,
                "region": self.setRegionOptions,
                "fuel": self.setFuelOptions,
            }
        )


class FindBusCFScore(FindSimpleYearRegion):
    __slots__ = []

    def setTableName(self):
        self.table_name = "sus_business_cfs"


class FindCombinedCFScore(FindSimpleYearRegion):
    __slots__ = []

    def setTableName(self):
        self.table_name = "sus_total_cfs"


class FiltersRiaDowWeekLocation(FiltersSimpleDayOfWeekWeekLocation):
    __slots__ = []

    def getFilteredDatesSQL(self):
        return self.get_filtered_dates_format(string_format=False)

    def setTableName(self):
        self.table_name = "coupon_redemption_ria01001"


class FindUberFilters(FindFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "week": {"label": "Week"},
            "day_of_week": {"label": "Day of Week"},
            "hierarchy2": {"label": "Location"},
        }

    def setDayOfWeekOptions(self):
        sql = """
        SELECT  DISTINCT TRIM(' ' FROM TO_CHAR(TO_TIMESTAMP(date, 'YYYY-MM-DD'), 'Day'))
                    AS day_of_week
        FROM (  SELECT  TO_CHAR(date, 'YYYY-MM-DD') AS date{week_select}
                FROM uber_applies_redemption
                WHERE date >= '{startDate}'
                  AND date <= '{endDate}'{filter_wc}) 
        ;"""
        sql = sql.format(
            metric=self.metric_object.params.metric,
            week_select=self.metric_object.params.gfie("week"),
            dow_select=self.metric_object.params.gfie("day_of_week"),
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df["day_of_week"])
        dowOrder = []
        [dowOrder.append(fList.index(i)) for i in day_order_dict.keys() if i in fList]
        fList = [fList[i] for i in dowOrder]
        self.metric_object.filters["day_of_week"] = fList

    def setWeekOptions(self):
        sql = """
        SELECT  DISTINCT DATE_TRUNC('week', TO_TIMESTAMP(date, 'YYYY-MM-DD')) AS week
        FROM (  SELECT  TO_CHAR(date, 'YYYY-MM-DD') AS date{dow_select}
                FROM uber_applies_redemption
                WHERE date >= '{startDate}'
                  AND date <= '{endDate}'{filter_wc})
        ORDER BY week
        ;"""
        sql = sql.format(
            metric=self.metric_object.params.metric,
            dow_select=self.metric_object.params.gfie("day_of_week"),
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df["week"])
        fList = [i.strftime("%Y-%m-%d") for i in fList]
        self.metric_object.filters["week"] = fList

    def getFilteredDatesSQL(self):
        sql = """
        SELECT *{week_select}{dow_select}
        FROM uber_applies_redemption
        WHERE TRUE{filter_wc}
        ORDER BY date
        ;"""
        # print(sql)
        sql = sql.format(
            week_select=self.metric_object.params.gfie("week"),
            dow_select=self.metric_object.params.gfie("day_of_week"),
            filter_wc=self.metric_object.params.filters_string,
            metric=self.metric_object.params.metric,
        )
        return sql

    def setGenericOptions(self, filterby):
        replace_filterby = filterby
        if filterby[:9] == "hierarchy":
            replace_filterby = f"{valid_hierarchies[filterby]} AS {filterby}"
        sql = """
        SELECT DISTINCT {replace_filterby}{week_select}{dow_select}
        FROM uber_applies_redemption
        WHERE date >= '{startDate}'
          AND date <= '{endDate}'{filter_wc}
        ORDER BY {filterby} 
        ;"""
        # Drop extra sql that would duplicate the filterby select(/join).
        filters_dict = deepcopy(self.metric_object.params.filters_dict)
        for grouping in filters_dict.keys():
            if grouping == filterby:
                for key in filters_dict[grouping].keys():
                    if key != "select":
                        filters_dict[grouping][key] = ""
                    else:
                        cut_starting = findNth(filters_dict[grouping][key], """,""", 2)
                        # part_1 = filters_dict[grouping][key][:cut_starting-1]
                        filters_dict[grouping][key] = filters_dict[grouping][key][
                            :cut_starting
                        ]
        sql = sql.format(
            filterby=filterby,
            replace_filterby=replace_filterby,
            week_select=self.metric_object.params.gfie("week"),
            dow_select=self.metric_object.params.gfie("day_of_week"),
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            metric=self.metric_object.params.metric,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[filterby])
        # Replace ' characters.
        fList = [s.replace("'", "\\'") if isinstance(s, str) else s for s in fList]
        fList = [i for i in fList if i is not None]
        self.metric_object.filters[filterby] = fList

    def setH2Options(self):
        self.setGenericOptions("hierarchy2")
        hh = HierarchicalHelpers(self.engine, self.metric_object)
        hh.parentDictFromNodeIds(self.metric_object.filters["hierarchy2"], "hierarchy2")

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "hierarchy2": self.setH2Options,
                "week": self.setWeekOptions,
                "day_of_week": self.setDayOfWeekOptions,
            }
        )


#######
#######
#######
class FindPostTrackingFormatFilters(FindTableName):
    __slots__ = []

    def getFilteredDatesSQL(self, get_count=False):
        sql = """
        SELECT {oc}DISTINCT m.date::DATE{cc} AS date
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        """
        sql_suffix = """
        WHERE m.field_ref LIKE '{field_ref}%%'{job_filter}{filter_wc}
        ORDER BY date
        ;"""
        if self.metric_object.params.field_ref is None:
            sql_suffix = """
        WHERE TRUE{job_filter}{filter_wc}
        ORDER BY date
        ;"""
        sql = sql + sql_suffix
        oc = ""
        cc = ""
        if get_count:
            oc = "COUNT("
            cc = ")"
        sql = sql.format(
            oc=oc,
            cc=cc,
            filter_wc=self.metric_object.params.filters_string,
            job_filter=self.metric_object.params.job_filter,
            table_name=self.table_name,
            field_ref=self.metric_object.params.field_ref,
        )
        # print(sql)
        return sql

    def setSimpleOption(self, opt_str):
        where_use_metric = f"m.field_ref LIKE '{self.metric_object.params.field_ref}%%'"
        if self.metric_object.params.field_ref is None:
            where_use_metric = "TRUE"
        sql = """
        SELECT  DISTINCT {opt_str}::VARCHAR
        FROM {table_name} m
        LEFT JOIN m.pt AS product_type ON TRUE
        WHERE {where_use_metric}
          AND TRUNC(date) >= '{startDate}'
          AND TRUNC(date) <= '{endDate}'
          AND {opt_str} IS NOT NULL{job_filter}{filter_wc}
        ORDER BY {opt_str}::VARCHAR
        ;"""
        sql = sql.format(
            opt_str=opt_str,
            where_use_metric=where_use_metric,
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            job_filter=self.metric_object.params.job_filter,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[opt_str])
        self.metric_object.filters[opt_str] = fList

    def setAgeGroupOptions(self):
        self.setSimpleOption("age_group")

    def setGenderOptions(self):
        self.setSimpleOption("gender")

    def setMonthsAfterOptions(self):
        self.setSimpleOption("months_after")

    def setPersonaOptions(self):
        self.setSimpleOption("persona")

    def setGenericOptions(self, filterby):
        where_use_metric = f"m.field_ref LIKE '{self.metric_object.params.field_ref}%%'"
        if self.metric_object.params.field_ref is None:
            where_use_metric = "TRUE"
        vh_at_hx = filterby
        if filterby in valid_hierarchies:
            vh_at_hx = valid_hierarchies[filterby]
        sql = """
        SELECT DISTINCT REPLACE({filterby}, CHR(37), 'pc.') AS {filterby}
        FROM (  SELECT  {vh_at_hx}::VARCHAR AS {filterby}
                FROM {table_name} m
                LEFT JOIN m.pt AS product_type ON TRUE
                WHERE {where_use_metric}
                    AND TRUNC(date) >= '{startDate}'
                    AND TRUNC(date) <= '{endDate}'{job_filter}{filter_wc}) fb
        WHERE {filterby} IS NOT NULL
        ORDER BY {filterby} 
        ;"""
        sql = sql.format(
            filterby=filterby,
            vh_at_hx=vh_at_hx,
            where_use_metric=where_use_metric,
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            job_filter=self.metric_object.params.job_filter,
            table_name=self.table_name,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[filterby])  # Replace ' characters.
        fList = [s.replace("'", "\\'") if isinstance(s, str) else s for s in fList]
        self.metric_object.filters[filterby] = fList

    # Here, we're figuring out how to find the valid hierarchy levels a user can use.
    def setH1Options(self):
        # This gets us all the hierarchy-1 options.
        self.setGenericOptions(
            "hierarchy1"
        )  # Get user-entered hierarchy values from RS
        hh = HierarchicalHelpers(self.engine, self.metric_object)
        hh.parentDictFromNodeIds(self.metric_object.filters["hierarchy1"], "hierarchy1")

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "age_group": {"label": "Age Group"},
            "gender": {"label": "Gender"},
            "months_after": {"label": "Months After"},
            "persona": {"label": "Persona"},
            "hierarchy1": {"label": "Product Type"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "age_group": self.setAgeGroupOptions,
                "gender": self.setGenderOptions,
                "months_after": self.setMonthsAfterOptions,
                "persona": self.setPersonaOptions,
                "hierarchy1": self.setH1Options,
            }
        )


class FindPostTrackingFilters(FindPostTrackingFormatFilters):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_post_tracking"


class FindPostTrackingDemoFilters(FindPostTrackingFormatFilters):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_post_tracking"


class FindPostTrackingFormatSkipMonthsAfterFilters(FindPostTrackingFormatFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "age_group": {"label": "Age Group"},
            "gender": {"label": "Gender"},
            "persona": {"label": "Persona"},
            "hierarchy1": {"label": "Product Type"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "age_group": self.setAgeGroupOptions,
                "gender": self.setGenderOptions,
                "persona": self.setPersonaOptions,
                "hierarchy1": self.setH1Options,
            }
        )


class FindPostTrackingSkipMonthsAfterFilters(
    FindPostTrackingFormatSkipMonthsAfterFilters
):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_post_tracking"


class FindPostTrackingDemoSkipMonthsAfterFilters(
    FindPostTrackingFormatSkipMonthsAfterFilters
):
    __slots__ = []

    def setTableName(self):
        self.table_name = "tf_demo_post_tracking"


class FindKaiduFilters(FindFilters):
    __slots__ = []

    def getFilteredDatesSQL(self):
        sql = """
        SELECT DISTINCT date
        FROM kaidu
        WHERE TRUE{job_filter}{filter_wc}
        ORDER BY date
        ;"""
        sql = sql.format(
            job_filter=self.metric_object.params.job_filter,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        return sql

    def setSimpleOption(self, opt_str):
        job_filter = self.metric_object.params.job_filter
        filters_string = self.metric_object.params.filters_string
        date_filter = f"""TRUNC(date) >= '{self.metric_object.params.startDate}'
          AND TRUNC(date) <= '{self.metric_object.params.endDate}'"""
        if self.metric_object.params.startDate == "None":
            date_filter = "TRUE"
        hour_limit = ""
        if opt_str == "hour":
            hour_limit = """
          AND hour IN ( '08:00', '09:00', '10:00', '11:00', '12:00',
                        '13:00', '14:00', '15:00', '16:00', '17:00', 'all')"""
        sql = f"""
        SELECT  DISTINCT {opt_str}::VARCHAR
        FROM kaidu
        WHERE {date_filter}{job_filter}{filters_string}{hour_limit}
        ORDER BY {opt_str}::VARCHAR
        ;"""
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[opt_str])
        self.metric_object.filters[opt_str] = fList

    def setZoneOptions(self):
        self.setSimpleOption("zone")

    def setHourOptions(self):
        self.setSimpleOption("hour")

    def setDayOfWeekOptions(self):
        self.setSimpleOption("day_of_week")
        fList = self.metric_object.filters["day_of_week"]
        dowOrder = []
        [dowOrder.append(fList.index(i)) for i in day_order_dict.keys() if i in fList]
        fList = [fList[i] for i in dowOrder]
        self.metric_object.filters["day_of_week"] = fList

    @abstractmethod
    def setValidGroupings(self):
        pass

    @abstractmethod
    def getValidFilters(self):
        pass


class FindKaiduDowHZFilters(FindKaiduFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "day_of_week": {"label": "Day of Week"},
            "hour": {"label": "Hour"},
            "zone": {"label": "Zone"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "day_of_week": self.setDayOfWeekOptions,
                "hour": self.setHourOptions,
                "zone": self.setZoneOptions,
            }
        )


class FindKaiduDowHFilters(FindKaiduFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "hour": {"label": "Hour"},
            "day_of_week": {"label": "Day of Week"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "day_of_week": self.setDayOfWeekOptions,
                "hour": self.setHourOptions,
            }
        )


class FindKaiduDowZFilters(FindKaiduFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "day_of_week": {"label": "Day of Week"},
            "zone": {"label": "Zone"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "day_of_week": self.setDayOfWeekOptions,
                "zone": self.setZoneOptions,
            }
        )


class FindSimpleFilters(FindTableName):
    __slots__ = []

    def getFilteredDatesSQL(self):
        sql = """
        SELECT DISTINCT date
        FROM {table_name} m
        WHERE TRUE{job_filter}{filter_wc}
        ORDER BY date
        ;"""
        sql = sql.format(
            table_name=self.table_name,
            job_filter=self.metric_object.params.job_filter,
            filter_wc=self.metric_object.params.filters_string,
        )
        # print(sql)
        return sql

    def setSimpleOption(self, opt_str):
        sql = """
        SELECT  DISTINCT {opt_str}::VARCHAR
        FROM {table_name} m
        WHERE TRUNC(date) >= '{startDate}'
          AND TRUNC(date) <= '{endDate}'{job_filter}{filter_wc}
        ORDER BY {opt_str}::VARCHAR
        ;"""
        sql = sql.format(
            opt_str=opt_str,
            table_name=self.table_name,
            startDate=self.metric_object.params.startDate,
            endDate=self.metric_object.params.endDate,
            filter_wc=self.metric_object.params.filters_string,
            job_filter=self.metric_object.params.job_filter,
        )
        # print(sql)
        df = pd.read_sql(sql, self.engine)
        fList = list(df[opt_str])
        self.metric_object.filters[opt_str] = fList

    @abstractmethod
    def setValidGroupings(self):
        pass

    @abstractmethod
    def getValidFilters(self):
        pass


class FindSquareFilters(FindSimpleFilters):
    __slots__ = []

    def setItemOptions(self):
        self.setSimpleOption("item")

    def setWeekOptions(self):
        self.setSimpleOption("week")

    def setCategoryOptions(self):
        self.setSimpleOption("category")

    def setDayOfWeekOptions(self):
        self.setSimpleOption("day_of_week")
        fList = self.metric_object.filters["day_of_week"]
        dowOrder = []
        [dowOrder.append(fList.index(i)) for i in day_order_dict.keys() if i in fList]
        fList = [fList[i] for i in dowOrder]
        self.metric_object.filters["day_of_week"] = fList

    def setTableName(self):
        self.table_name = "square_o365"


class FindAllSquareFilters(FindSquareFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "day_of_week": {"label": "Day of Week"},
            "week": {"label": "Week"},
            # 'item':{'label':'Item'},
            "category": {"label": "Category"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "day_of_week": self.setDayOfWeekOptions,
                "week": self.setWeekOptions,
                # 'item':self.setItemOptions,
                "category": self.setCategoryOptions,
            }
        )


# class FindSquareDWIFilters(FindSquareFilters):
#     __slots__ = []
#     def setValidGroupings(self):
#         self.metric_object.valid_groupings = {
#             'day_of_week':{'label':'Day of Week'},
#             'week':{'label':'Week'},
#             'item':{'label':'Item'}
#         }
#     def getValidFilters(self):
#         self.setValidFilters({  'date':self.setFilteredDates,
#                                 'day_of_week':self.setDayOfWeekOptions,
#                                 'week':self.setWeekOptions,
#                                 'item':self.setItemOptions})


class FindSquareDWFilters(FindSquareFilters):
    __slots__ = []

    def setValidGroupings(self):
        self.metric_object.valid_groupings = {
            "day_of_week": {"label": "Day of Week"},
            "week": {"label": "Week"},
        }

    def getValidFilters(self):
        self.setValidFilters(
            {
                "date": self.setFilteredDates,
                "day_of_week": self.setDayOfWeekOptions,
                "week": self.setWeekOptions,
            }
        )


###########################
### /end Filter Options ###
###########################

########################################
### Hierarchical groupings functions ###
########################################


def findRowInDct(dct, row):
    for key in dct.keys():
        for i in range(0, len(dct[key])):
            if i == 0:
                if dct[key][0] == row["parent_id"]:
                    dct[key].append({row["node_name"]: [row["node_id"]]})
                    return True
            else:
                found = findRowInDct(dct[key][i], row)
                if found is True:
                    return True
    return False


#############################################
### /end Hierarchical groupings functions ###
#############################################

################################
### Supplementary Dicitonary ###
################################

day_order_dict = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6,
}


#####################################
### /end Supplementary Dictionary ###
#####################################
