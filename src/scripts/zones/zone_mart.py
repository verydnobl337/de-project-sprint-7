from pyspark.sql import functions as F
from pyspark.sql.window import Window


class ZoneMartBuilder:

    def __init__(self, df):
        self.df = df

    def build(self):

        df = self.df.withColumn("ts", F.to_timestamp("ts"))

        # определяю первое событие пользователя
        user_window = Window.partitionBy("user_id").orderBy("ts")

        df = (
            df.withColumn(
                "rn",
                F.row_number().over(user_window)
            )
            .withColumn(
                "is_registration",
                F.when(F.col("rn") == 1, 1).otherwise(0)
            )
            .drop("rn")
        )

        # календарные признаки
        df = (
            df.withColumn(
                "month",
                F.date_format("ts", "yyyy-MM")
            )
            .withColumn(
                "week",
                F.date_trunc("week", "ts")
            )
        )

        week_agg = (
            df.groupBy("week", "city")
            .agg(

                F.count(
                    F.when(
                        F.col("event_type") == "message",
                        1
                    )
                ).alias("week_message"),

                F.count(
                    F.when(
                        F.col("event_type") == "reaction",
                        1
                    )
                ).alias("week_reaction"),

                F.count(
                    F.when(
                        F.col("event_type") == "subscription",
                        1
                    )
                ).alias("week_subscription"),

                F.sum("is_registration")
                .alias("week_registrations")
            )
        )

        month_agg = (
            df.groupBy("month", "city")
            .agg(

                F.count(
                    F.when(
                        F.col("event_type") == "message",
                        1
                    )
                ).alias("month_message"),

                F.count(
                    F.when(
                        F.col("event_type") == "reaction",
                        1
                    )
                ).alias("month_reaction"),

                F.count(
                    F.when(
                        F.col("event_type") == "subscription",
                        1
                    )
                ).alias("month_subscription"),

                F.sum("is_registration")
                .alias("month_registrations")
            )
        )

        result = (
            week_agg
            .join(
                month_agg,
                week_agg.city == month_agg.city,
                "outer"
            )
            .drop(month_agg.city)
            .withColumnRenamed(
                "city",
                "zone_id"
            )
        )

        return result
