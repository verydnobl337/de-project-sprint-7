from pyspark.sql import functions as F
from pyspark.sql.window import Window

from haversine import Haversine


class FriendRecommendationBuilder:
    def __init__(self, df):
        self.df = df

    def build(self):

        base = (
            self.df
            .select(
                "user_id",
                "city",
                "ts",
                "event_type",
                "event_lat",
                "event_lon",
                "message_from",
                "message_to",
                "subscription_channel",
                "subscription_user"
            )
            .filter(F.col("user_id").isNotNull())
        )

        # Последняя известная геопозиция пользователя
        w = Window.partitionBy("user_id").orderBy(
            F.col("ts").desc()
        )

        user_last_pos = (
            base
            .withColumn(
                "rn",
                F.row_number().over(w)
            )
            .filter(F.col("rn") == 1)
            .select(
                "user_id",
                "city",
                "event_lat",
                "event_lon"
            )
        )

        # Пользователи одного канала
        subs = (
            base
            .filter(
                F.col("event_type") == "subscription"
            )
            .select(
                F.col("subscription_user").alias("user_id"),
                "subscription_channel"
            )
            .dropDuplicates()
        )

        subs_pairs = (
            subs.alias("a")
            .join(
                subs.alias("b"),
                "subscription_channel"
            )
            .where(
                F.col("a.user_id") < F.col("b.user_id")
            )
            .select(
                F.col("a.user_id").alias("user_left"),
                F.col("b.user_id").alias("user_right"),
                F.col("subscription_channel")
            )
            .dropDuplicates()
        )

        # Пользователи, которые уже переписывались
        msg_pairs = (
            base
            .filter(
                F.col("event_type") == "message"
            )
            .select(
                F.least(
                    "message_from",
                    "message_to"
                ).alias("user_left"),

                F.greatest(
                    "message_from",
                    "message_to"
                ).alias("user_right")
            )
            .filter(
                F.col("user_left").isNotNull()
            )
            .dropDuplicates()
        )

        # Добавляю координаты пользователей
        left = user_last_pos.alias("l")
        right = user_last_pos.alias("r")


        pairs = (
            subs_pairs

            .join(
                left,
                left.user_id == subs_pairs.user_left
            )

            .join(
                right,
                right.user_id == subs_pairs.user_right
            )

            .select(
                subs_pairs.user_left,
                subs_pairs.user_right,
                subs_pairs.subscription_channel,

                F.col("l.city").alias("zone_id"),

                F.col("l.event_lat").alias("lat1"),
                F.col("l.event_lon").alias("lon1"),

                F.col("r.event_lat").alias("lat2"),
                F.col("r.event_lon").alias("lon2")
            )
        )

        # Расстояние между пользователями
        pairs = pairs.withColumn(
            "distance",
            Haversine.distance(
                F.col("lat1"),
                F.col("lon1"),
                F.col("lat2"),
                F.col("lon2")
            )
        )

        # Убираю тех, кто уже общался
        pairs = (
            pairs
            .join(
                msg_pairs,
                [
                    "user_left",
                    "user_right"
                ],
                "left_anti"
            )
        )

        # Только ближе километра
        pairs = pairs.filter(
            F.col("distance") <= 1
        )

        result = pairs.select(
            "user_left",
            "user_right",
            F.current_timestamp().alias(
                "processed_dttm"
            ),
            "zone_id",
            F.current_timestamp().alias(
                "local_time"
            )
        )

        return result