from pyspark.sql import functions as F
from pyspark.sql.window import Window


class ZoneMartBuilder:

    def __init__(self, df):
        self.df = df

    def build(self):

        df = self.df.withColumn("ts", F.to_timestamp("ts"))

        # календарные признаки
        df = df.withColumn("month", F.date_format("ts", "yyyy-MM")) \
               .withColumn("week", F.date_trunc("week", "ts"))

        week_agg = df.groupBy("week", "city").agg(

            F.count(F.when(F.col("event_type") == "message", 1)).alias("week_message"),
            F.count(F.when(F.col("event_type") == "reaction", 1)).alias("week_reaction"),
            F.count(F.when(F.col("event_type") == "subscription", 1)).alias("week_subscription"),

            F.countDistinct("user_id").alias("week_user")
        )

        month_agg = df.groupBy("month", "city").agg(

            F.count(F.when(F.col("event_type") == "message", 1)).alias("month_message"),
            F.count(F.when(F.col("event_type") == "reaction", 1)).alias("month_reaction"),
            F.count(F.when(F.col("event_type") == "subscription", 1)).alias("month_subscription"),

            F.countDistinct("user_id").alias("month_user")
        )

        w = Window.partitionBy("user_id").orderBy("ts")

        reg_df = (
            df.withColumn("rn", F.row_number().over(w))
              .filter(F.col("rn") == 1)
              .groupBy("city")
              .agg(F.count("user_id").alias("registrations"))
        )

        result = (
            week_agg
            .join(month_agg, week_agg.city == month_agg.city, "outer")
            .drop(month_agg.city)
            .withColumnRenamed("city", "zone_id")
            .join(reg_df.withColumnRenamed("city", "zone_id"), "zone_id", "left")
        )

        return result