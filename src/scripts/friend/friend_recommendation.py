from pyspark.sql import functions as F
from pyspark.sql.window import Window
from haversine import Haversine


class FriendRecommendationBuilder:

    def __init__(self, df):
        self.df = df

    def build(self):

        base = self.df.select(
            "user_id",
            "city",
            "ts",
            "event_type",
            "event_lat",
            "event_lon"
        ).filter(F.col("user_id").isNotNull())

        w = Window.partitionBy("user_id").orderBy(F.col("ts").desc())

        user_last_pos = (
            base.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .select(
                "user_id",
                "city",
                "event_lat",
                "event_lon"
            )
        )

        subs = (
            base.filter(F.col("event_type") == "subscription")
            .select("user_id", "city")
            .dropDuplicates()
        )

        subs_pairs = (
            subs.alias("a")
            .join(subs.alias("b"), "city")
            .where(F.col("a.user_id") < F.col("b.user_id"))
            .select(
                F.col("a.user_id").alias("user_left"),
                F.col("b.user_id").alias("user_right"),
                F.col("city")
            )
            .dropDuplicates()
        )

        messages = (
            base.filter(F.col("event_type") == "message")
            .select(
                F.col("user_id"),
                F.col("city")
            )
            .dropDuplicates()
        )

        msg_pairs = (
            messages.alias("a")
            .join(messages.alias("b"), "city")
            .where(F.col("a.user_id") < F.col("b.user_id"))
            .select(
                F.col("a.user_id").alias("user_left"),
                F.col("b.user_id").alias("user_right")
            )
            .dropDuplicates()
        )

        left = user_last_pos.alias("l")
        right = user_last_pos.alias("r")

        pairs = (
            subs_pairs
            .join(left, left.user_id == subs_pairs.user_left)
            .join(right, right.user_id == subs_pairs.user_right)
            .select(
                subs_pairs.user_left,
                subs_pairs.user_right,
                subs_pairs.city,
                F.col("l.event_lat").alias("lat1"),
                F.col("l.event_lon").alias("lon1"),
                F.col("r.event_lat").alias("lat2"),
                F.col("r.event_lon").alias("lon2")
            )
        )

        pairs = pairs.withColumn(
            "distance",
            Haversine.distance(
                F.col("lat1"),
                F.col("lon1"),
                F.col("lat2"),
                F.col("lon2")
            )
        )

        pairs = pairs.join(
            msg_pairs,
            ["user_left", "user_right"],
            "left_anti"
        )

        pairs = pairs.filter(F.col("distance") <= 1)

        result = pairs.select(
            "user_left",
            "user_right",
            F.current_timestamp().alias("processed_dttm"),
            F.col("city").alias("zone_id"),
            F.current_timestamp().alias("local_time")
        )

        return result