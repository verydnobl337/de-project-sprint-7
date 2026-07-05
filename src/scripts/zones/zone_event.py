from pyspark.sql import functions as F


class ZoneEventsBuilder:

    def __init__(self, city_matched_df):
        self.df = city_matched_df

    def build(self):

        # оставляю только нужные поля для витрины
        df = self.df.select(
            F.col("user_id"),
            F.col("city").alias("zone_id"),
            F.to_timestamp("ts").alias("ts"),
            F.col("event_type")
        )

        # оставляю только события, которые участвуют в витрине
        result = df.filter(
            F.col("event_type").isin("message", "reaction", "subscription")
        )

        return result