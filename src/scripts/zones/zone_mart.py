from pyspark.sql import functions as F


class ZoneMartBuilder:

    def __init__(self, df):
        self.df = df

    def build(self):

        df = self.df.withColumn("ts", F.to_timestamp("ts"))

        df = df.withColumn("month", F.date_format("ts", "yyyy-MM")) \
               .withColumn("week", F.weekofyear("ts")) \
               .withColumn("year", F.year("ts"))

        df = df.withColumn(
            "week_key",
            F.concat_ws("-", F.col("year"), F.col("week"))
        )

        result = df.groupBy("week_key", "month", "zone_id").agg(

            F.sum(F.when(F.col("event_kind") == "message", 1).otherwise(0)).alias("week_message"),
            F.sum(F.when(F.col("event_kind") == "reaction", 1).otherwise(0)).alias("week_reaction"),
            F.sum(F.when(F.col("event_kind") == "subscription", 1).otherwise(0)).alias("week_subscription"),

            F.countDistinct(F.col("user_id")).alias("week_user"),

            F.sum(F.when(F.col("event_kind") == "message", 1).otherwise(0)).alias("month_message"),
            F.sum(F.when(F.col("event_kind") == "reaction", 1).otherwise(0)).alias("month_reaction"),
            F.sum(F.when(F.col("event_kind") == "subscription", 1).otherwise(0)).alias("month_subscription"),

            F.countDistinct(F.col("user_id")).alias("month_user")
        )

        return result