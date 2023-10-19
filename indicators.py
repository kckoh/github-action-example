from abc import ABC, abstractmethod
import pandas as pd
from copy import deepcopy
from math import isnan
from datetime import datetime

from parameter_setters import Params
from helpers import (
    human_format,
    getMetricAndParamClasses,
    get_previous_date,
    get_last_saturday,
)

#########################
### Indicator Classes ###
#########################


class Indicator(ABC):
    # !!! apply __slots__ to Indicator class, subclasses.
    def __init__(
        self, df: pd.DataFrame, metric_params: Params, below_target_good=False
    ):
        self.below_target_good = below_target_good
        self.df = df
        self.metric_params = metric_params

    @abstractmethod
    def get_indicator(self, row):
        pass

    @abstractmethod
    def applyIndicator(self, engine, metric_dict, jmd):
        pass


class IndicatorVsIntervals(Indicator):
    def get_indicator(self, row, col, intervals, labels, row_colour, row_value):
        row["indicator_colour"] = row_colour
        row["indicator_value"] = row_value
        for colour in intervals:
            for interval in intervals[colour]:
                if (row[col] >= interval[0]) and (row[col] < interval[1]):
                    row["indicator_colour"] = colour
                    row["indicator_value"] = labels[colour]
        return row

    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        col = self.metric_params.metric
        intervals = jmd[self.metric_params.job][col]["target_intervals"]
        labels = jmd[self.metric_params.job][col]["interval_labels"]
        row_colour = list(set(labels.keys()) - set(intervals.keys()))[0]
        row_value = labels[row_colour]
        df = df.apply(
            self.get_indicator,
            axis=1,
            col=col,
            intervals=intervals,
            labels=labels,
            row_colour=row_colour,
            row_value=row_value,
        )
        df["indicator_label"] = "Vs Intervals:"
        print(df)
        return df


# row-level indicator-colour/values
class IndicatorDefault(Indicator):
    def get_indicator(self, row):
        col = self.metric_params.metric
        print(
            f'row: {row}, \n row["target"]: {row["target"]}, col: {col}, row[col]: {row[col]}'
        )
        if row["target"] == 0:
            indicator_value = "n/a (/0)"
            indicator_colour = "amber"
        elif (row[col] is None) or (row["target"] is None):
            indicator_value = "Missing"
            indicator_colour = "red"
        else:
            div = row[col] / row["target"]
            if div > 1:
                indicator_value = "+" + human_format(div * 100 - 100, 1) + "%"
                indicator_colour = "red" if self.below_target_good else "green"
            else:
                indicator_value = human_format(div * 100 - 100, 1) + "%"
                indicator_colour = "amber"
                if div < 1:
                    indicator_colour = "green" if self.below_target_good else "red"
        self.indicator_value = indicator_value
        self.indicator_colour = indicator_colour
        return (indicator_value, indicator_colour)


class IndicatorExec(Indicator):
    def get_indicator(self, row):
        col = self.metric_params.metric
        if row["target"] == 0:
            indicator_value = "N/A"
            indicator_clr = "amber"
        elif (row[col] is None) or (isnan(row[col])):
            indicator_value = "N/A"
            indicator_clr = "amber"
        else:
            div = row[col] / row["target"]
            if div > 1:
                indicator_value = "+" + human_format(div * 100 - 100, 1) + "%"
                if div > 1.1:
                    indicator_clr = "red" if self.below_target_good else "green"
                else:
                    indicator_clr = "amber" if self.below_target_good else "light-green"
            else:
                indicator_value = human_format(div * 100 - 100, 1) + "%"
                if div <= 0.9:
                    indicator_clr = "green" if self.below_target_good else "red"
                else:
                    indicator_clr = "light-green" if self.below_target_good else "amber"
        self.indicator_value = indicator_value
        self.indicator_clr = indicator_clr
        return (indicator_value, indicator_clr)


class IndicatorPie(Indicator):
    def get_indicator(self, row):
        df = self.df
        if df["target"][0] == 0:
            indicator_value = "Divide by 0 error."
            indicator_clr = "amber"
        else:
            row = df[df["count"] == df["count"].max()].head(1)
            div = ((100 * row["count"] / row["size"]) / row["target"]).values[0]
            if div > 1:
                indicator_value = "+" + human_format(100 * div - 100, 1) + "%"
                indicator_clr = "red" if self.below_target_good else "green"
            else:
                indicator_value = human_format(100 * div - 100, 1) + "%"
                indicator_clr = "amber"
                if div < 1:
                    indicator_clr = "green" if self.below_target_good else "red"
        self.indicator_value = indicator_value
        self.indicator_clr = indicator_clr
        return (indicator_value, indicator_clr)


# df-level indicators
class IndicatorStockLevel(IndicatorDefault):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        df["indicator_label"] = "Locations needing resupply:"
        df["indicator_value"] = str(df["resupply"][0])
        if df["resupply"][0] == 0:
            df["indicator_colour"] = "green"
        else:
            df["indicator_colour"] = "red"
        return df


class IndicatorDefaultToTarget(IndicatorDefault):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        if len(df) > 0:
            df["indicator_value"], df["indicator_colour"] = zip(
                *df.apply(self.get_indicator, axis=1)
            )
            df["indicator_label"] = "Versus Benchmark"
        else:
            df["indicator_colour"] = "amber"
            df["indicator_label"] = "No data available."
            df["indicator_value"] = ""
        return df


class IndicatorExecToTarget(IndicatorExec):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        if len(df) > 0:
            df["indicator_value"], df["indicator_colour"] = zip(
                *df.apply(self.get_indicator, axis=1)
            )
            df["indicator_label"] = "Versus Benchmark"
        else:
            df["indicator_colour"] = "amber"
            df["indicator_label"] = "No data available."
            df["indicator_value"] = ""
        return df


class IndicatorDefaultToPrevDay(IndicatorDefault):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        # get end-last valid date.
        input_copy = deepcopy(self.metric_params.ai)
        input_copy["endDate"] = self.metric_params.endDate
        input_copy["startDate"] = input_copy["endDate"]  # Get only last date's data.
        imp, imc = getMetricAndParamClasses(
            input_copy, metric_dict, jmd, self.metric_params.hash_dict
        )
        i_metric_params = imp(
            engine, input_copy, self.metric_params.hash_dict, metric_dict, jmd
        )
        comparison_date = i_metric_params.getMaxDate(
            engine, last_date=f"'{i_metric_params.endDate}'"
        )
        print("comparison_date: ", comparison_date)
        # Run basic query on only the last day.
        i_metric_class = imc(engine, i_metric_params, metric_dict, jmd)
        i_metric_class.setL1()
        most_recent_value = i_metric_class.df[i_metric_params.metric][0]
        most_recent_date = i_metric_params.endDate
        # None-type gets converted to str() at end of .getMaxDate().
        if comparison_date == "None":
            df["indicator_value"] = "No p.v.d."
            df["indicator_colour"] = "amber"
            df["indicator_label"] = "Vs Previous Valid Date"
            df["previous_valid_date"] = "None"
            df["previous_value"] = "N/A"
        else:
            # Same thing but for previous valid day.
            i_metric_params.startDate = comparison_date
            i_metric_params.endDate = comparison_date
            i_metric_class.setL1()
            comparison_value = i_metric_class.df[i_metric_params.metric][0]
            dx = pd.DataFrame(
                {
                    i_metric_params.metric: [most_recent_value],
                    "target": [comparison_value],
                }
            )
            self.get_indicator(dx.loc[0])
            df["indicator_value"] = self.indicator_value
            df["indicator_colour"] = self.indicator_colour
            df["indicator_label"] = "Vs Previous Valid Date"
            df["previous_valid_date"] = comparison_date
            df["previous_value"] = comparison_value
        df["most_recent_date"] = most_recent_date
        df["most_recent_value"] = most_recent_value
        return df


def startEndString(start, end):
    return start + " -> " + end


class IndicatorDefaultPrevSaturday(IndicatorDefault):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        current_value = df[self.metric_params.metric][0]
        # get previous week.
        input_copy = deepcopy(self.metric_params.ai)
        input_copy["endDate"] = get_previous_date(self.metric_params.endDate)
        input_copy["startDate"] = get_last_saturday(
            datetime.strptime(self.metric_params.startDate, "%Y-%m-%d").date()
        )
        params_class, metric_class = getMetricAndParamClasses(
            input_copy, metric_dict, jmd, self.metric_params.hash_dict
        )
        comp_params = params_class(
            engine, input_copy, self.metric_params.hash_dict, metric_dict, jmd
        )
        # Run basic query on only the last day.
        comp_metric = metric_class(engine, comp_params, metric_dict, jmd)
        comp_metric.setL1()
        df["previous_week"] = startEndString(
            input_copy["startDate"], input_copy["endDate"]
        )
        df["indicator_label"] = "Vs Previous Saturday-week"
        if comp_metric.df.empty:
            df["indicator_value"] = "No prev. value"
            df["indicator_colour"] = "amber"
            df["previous_value"] = "N/A"
        else:
            # Same thing but for previous valid day.
            comp_value = comp_metric.df[comp_params.metric][0]
            dx = pd.DataFrame(
                {
                    comp_params.metric: [current_value],
                    "target": [comp_value],
                }
            )
            self.get_indicator(dx.loc[0])
            df["indicator_value"] = self.indicator_value
            df["indicator_colour"] = self.indicator_colour
            df["previous_value"] = comp_value
        df["current_week"] = startEndString(
            self.metric_params.startDate, self.metric_params.endDate
        )
        df["current_value"] = current_value
        return df


class IndicatorSumVsProjectAverage(IndicatorDefault):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        mc_instantiated = metric_dict[self.metric_params.metric]["metric_class"](
            engine, self.metric_params, metric_dict, jmd
        )
        sql = mc_instantiated.ff.getFilteredDatesSQL(get_count=True)
        dt_cnt = pd.read_sql(sql, engine)  # count of valid dates.
        dt_cnt = dt_cnt["date"][0]
        er_inputs = deepcopy(self.metric_params.ai)  # er --> entire range
        # Because this is called on L1, this will be the last valid date.
        er_inputs["endDate"] = self.metric_params.endDate
        er_inputs["startDate"] = self.metric_params.getMinDate(engine)
        imp, imc = getMetricAndParamClasses(
            er_inputs, metric_dict, jmd, self.metric_params.hash_dict
        )
        er_params = imp(
            engine, er_inputs, self.metric_params.hash_dict, metric_dict, jmd
        )  # parameters set
        er_metric = imc(engine, er_params, metric_dict, jmd)  # metric set
        er_metric.setL1()
        er_value = er_metric.df[er_params.metric][0]
        er_value = er_value / dt_cnt
        col = self.metric_params.metric
        dx = pd.DataFrame({col: [df[col][0]], "target": [er_value]})
        row = dx.loc[0]
        iv, ic = self.get_indicator(row)
        df["indicator_value"] = iv
        df["indicator_colour"] = ic
        df["indicator_label"] = "Versus Average Day"
        df["average_day"] = int(er_value)
        return df


class IndicatorAvgVsProjectAverage(IndicatorDefault):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        er_inputs = deepcopy(self.metric_params.ai)  # er --> entire range
        er_inputs["endDate"] = self.metric_params.endDate
        er_inputs["startDate"] = self.metric_params.getMinDate(engine)
        imp, imc = getMetricAndParamClasses(
            er_inputs, metric_dict, jmd, self.metric_params.hash_dict
        )
        er_params = imp(
            engine, er_inputs, self.metric_params.hash_dict, metric_dict, jmd
        )  # parameters set
        er_metric = imc(engine, er_params, metric_dict, jmd)  # metric set
        er_metric.setL1()
        try:
            er_value = er_metric.df[er_params.metric][0]
            # er_value = er_value/dt_cnt
            col = self.metric_params.metric
            dx = pd.DataFrame({col: [df[col][0]], "target": [er_value]})
            row = dx.loc[0]
            iv, ic = self.get_indicator(row)
            df["indicator_value"] = iv
            df["indicator_colour"] = ic
            df["indicator_label"] = "Versus Average Day"
            df["average_day"] = int(er_value)
        except IndexError:
            df["indicator_value"] = "0"
            df["indicator_colour"] = "yellow"
            df["indicator_label"] = "Versus Average Day"
            df["average_day"] = 0
        return df


class IndicatorAvgVsOverallAverage(IndicatorDefault):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        er_inputs = deepcopy(self.metric_params.ai)  # er --> entire range
        er_inputs["endDate"] = self.metric_params.endDate
        er_inputs["startDate"] = self.metric_params.getMinDate(engine)
        # If it's demo, need the job to ensure the right param/metric classes are used.
        if "job" in er_inputs.keys():
            if (
                er_inputs["job"]
                != "a34b59d23b01bb9f00f32e40b5b6e1a9bb8f532f1a23b8a925f173a9561fc04d"
            ):
                del er_inputs["job"]
        imp, imc = getMetricAndParamClasses(
            er_inputs, metric_dict, jmd, self.metric_params.hash_dict
        )
        er_params = imp(
            engine, er_inputs, self.metric_params.hash_dict, metric_dict, jmd
        )  # parameters set
        er_metric = imc(engine, er_params, metric_dict, jmd)  # metric set
        er_metric.setL1()
        er_value = er_metric.df[er_params.metric][0]
        col = self.metric_params.metric
        dx = pd.DataFrame({col: [df[col][0]], "target": [er_value]})
        row = dx.loc[0]
        iv, ic = self.get_indicator(row)
        df["indicator_value"] = iv
        df["indicator_colour"] = ic
        df["indicator_label"] = "Versus Overall Average Day"
        df["overall_average_day"] = int(er_value)
        return df


class IndicatorPercentOfTotal(IndicatorDefault):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df.copy()
        col = self.metric_params.metric
        total = df["count"].sum()
        df = df[df[col] != "Unanswered"]
        df = df[df["count"] == df["count"].max()]
        df = df.reset_index(drop=True)
        df = df.loc[[0]]
        iv = round(df["count"] * 100.0 / total, 0)
        df["indicator_value"] = iv.astype("int64").astype(str) + "%"
        df["indicator_label"] = "Percent of Responses"
        df["indicator_colour"] = "green"
        # print("indicator's df")
        # print(df)
        return df


class IndicatorVsPrevTime(IndicatorExec):
    def time_indicator(self, engine, metric_dict, jmd, time_period, indicator, label):
        df = self.df
        col = self.metric_params.metric
        # Assumes only two rows in df, 0 and 1.
        previous_full = df[col][0]
        df["previous_value"] = previous_full

        # Set the 'target' column to the previous full value
        df["target"] = previous_full

        # Get the current month and the last month
        last_range = (
            datetime.utcnow().replace(day=1) - pd.offsets.DateOffset(months=time_period)
        ).strftime("%Y-%m")

        if len(df) > 1:
            row = df.loc[1]
            iv, ic = self.get_indicator(row)
            # Set the indicator value and colour for the single row
            df = df.iloc[[1]]
            df["indicator_value"] = iv
            df["indicator_colour"] = ic
        elif df[indicator][0] == last_range:
            # Set values for the case when there is no 2nd row & it's the last month
            df["indicator_value"] = "N/A"
            df["indicator_colour"] = "amber"
            df["previous_value"] = "No data"
        else:
            # Set values for the case when there is no 2nd row & it's not the last month
            df["indicator_value"] = "N/A"
            df["indicator_colour"] = "amber"
            df[col][0] = "No data / 0"

        # Set other common columns
        df["indicator_label"] = label
        df.drop(["target"], inplace=True, axis=1)
        df.reset_index(drop=True, inplace=True)

        # print("IndicatorVsPrevQuarter()'s df: ")
        # print(df)
        return df


class IndicatorVsPrevMonth(IndicatorVsPrevTime):
    def applyIndicator(self, engine, metric_dict, jmd):
        return self.time_indicator(
            engine,
            metric_dict,
            jmd,
            time_period=1,
            indicator="month",
            label="Vs Previous Month",
        )


class IndicatorVsPrevQuarter(IndicatorVsPrevTime):
    def applyIndicator(self, engine, metric_dict, jmd):
        return self.time_indicator(
            engine,
            metric_dict,
            jmd,
            time_period=3,
            indicator="quarter",
            label="Vs Previous Quarter",
        )


class IndicatorVsPrev30(IndicatorExec):
    def applyIndicator(self, engine, metric_dict, jmd):
        df = self.df
        row = df.loc[0]  # Assumes only two rows in df, 0 and 1.
        iv, ic = self.get_indicator(row)
        df["indicator_value"] = iv
        df["indicator_colour"] = ic
        df["indicator_label"] = "Vs Days 31-60"
        df["previous_value"] = df["target"]
        df.drop(["target"], inplace=True, axis=1)
        # print("IndicatorVsPrev30()'s df: ")
        # print(df)
        return df
