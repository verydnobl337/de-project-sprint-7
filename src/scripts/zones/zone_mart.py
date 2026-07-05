from pyspark.sql import functions as F


class ZoneMartBuilder:

    def __init__(self, df):
        self.df = df

    def build(self):

        # привожу время
        df = self.df.withColumn("ts", F.to_timestamp("ts"))

        # календарные признаки
        df = df.withColumn("month", F.date_format("ts", "yyyy-MM")) \
               .withColumn("week", F.date_format(F.to_date("ts"), "yyyy-ww"))

        # агрегирую по зонам, неделям и месяцам
        agg = df.groupBy("month", "week", "zone_id").agg(

            # неделя
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("week_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("week_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("week_subscription"),
            F.countDistinct(
                F.when(F.col("event_type") == "message", F.col("user_id"))
            ).alias("week_user"),

            # месяц
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("month_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("month_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("month_subscription"),
            F.countDistinct(
                F.when(F.col("event_type") == "message", F.col("user_id"))
            ).alias("month_user")
        )

        return agg