from abc import ABC, abstractmethod

######################
### Target Classes ###
######################


class TargetSQL(ABC):
    __slots__ = ["target", "target_sql", "target_cte", "target_join"]

    def __init__(self, target):
        self.target = target
        self.target_sql = ""
        self.target_cte = ""
        self.target_join = ""

    @abstractmethod
    def setTargetSQL(self):
        pass

    def getTargetSQL(self):
        return self.target_sql, self.target_cte, self.target_join


class TargetSQLCTENewHiresRegion(TargetSQL):
    def setTargetSQL(self):
        self.target_cte = f"""
        target_cte AS (
            SELECT  grouping_value, 
                    target AS region_target,
                    {self.target} AS target
            FROM  metric_grouping_target
            WHERE metric_name = 'new_hires'
        ),
        """

        self.target_join = f"""LEFT JOIN target_cte tc ON tc.grouping_value = g.region
        """

        self.target_sql = f", region_target, target"
        return False, False


class TargetSQLSimple(TargetSQL):
    __slots__ = []

    def setTargetSQL(self):
        self.target_sql = f", {self.target} AS target"
        return False, False


class TargetSQLbyLocation(TargetSQL):
    # ["ps_report_posted"]
    __slots__ = []

    def setTargetSQL(self):
        self.target_sql = f", (COUNT(DISTINCT location) * {self.target}) AS target"
        return True, False


class TargetSQLbyLocationDate(TargetSQL):
    # (("job" in api_input.keys()) and (api_input["job"] in ["RIA01001"]) and (metric in ["daily_transaction", "coupon_redemption", "survey_submissions"]))
    __slots__ = []

    def setTargetSQL(self):
        self.target_sql = f", (COUNT(DISTINCT (date::VARCHAR + 'separator' +  location)) * {self.target}) AS target"
        return True, False  # include_location


class TargetSQLbyProductDate(TargetSQL):
    # (("job" in api_input.keys()) and (api_input["job"] in ["RIA01001"]) and (metric in ["daily_transaction", "coupon_redemption", "survey_submissions"]))
    __slots__ = []

    def setTargetSQL(self):
        self.target_sql = f", (COUNT(DISTINCT date::VARCHAR) * COUNT(DISTINCT product_type) * {self.target}) AS target"
        return False, True


class TargetSQLbyResponse(TargetSQL):
    # ["waste_management", "issues_boolean", "recommendations_boolean", "kit_issues_boolean",
    #  "social_mentions_strength", "social_mentions_passion", "social_mentions_reach"]
    __slots__ = []

    def setTargetSQL(self):
        self.target_sql = f", (COUNT(DISTINCT response_id) * {self.target}) AS target"
        return False, False


class TargetSQLbyCustomerId(TargetSQL):
    # ["revenue_per_lead"]
    __slots__ = []

    def setTargetSQL(self):
        self.target_sql = f", (COUNT(DISTINCT customer_id) * {self.target}) AS target"
        return False, False


class TargetSQLSum(TargetSQL):
    # ["footfall", "daily_transaction"]
    __slots__ = []

    def setTargetSQL(self):
        self.target_sql = f", SUM({self.target}) AS target"
        return False, False


class TargetSQLbyDate(TargetSQL):
    # ["cards_distributed"]
    __slots__ = []

    def setTargetSQL(self):
        self.target_sql = f", (COUNT(DISTINCT date) * {self.target}) AS target"
        return False, False


###########################
### /end Target Classes ###
###########################
