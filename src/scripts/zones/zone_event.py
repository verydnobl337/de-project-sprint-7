from pyspark.sql import functions as F


class ZoneEventsBuilder:

    def __init__(self, city_matched_df):
        self.df = city_matched_df

    def build(self):

        df = self.df.select(
            F.col("user_id"),
            F.col("city").alias("zone_id"),
            F.col("ts"),
            F.col("event_type")
        )

        df = df.filter(F.col("user_id").isNotNull())

        df = df.filter(
            F.col("event_type").isin(
                "message",
                "reaction",
                "subscription"
            )
        )

        return df