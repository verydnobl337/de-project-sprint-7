from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

from haversine import Haversine


class CityMatcher:

    def __init__(self, events_df, cities_df):
        self.events_df = events_df
        self.cities_df = cities_df

    def match(self):

        events = self.events_df.select(

            # универсальный пользователь
            F.coalesce(
                F.col("event.message_from"),
                F.col("event.user").cast("long")
            ).alias("user_id"),

            # сообщение
            F.col("event.message_from").alias("message_from"),
            F.col("event.message_to").alias("message_to"),
            F.col("event.message_id").alias("message_id"),

            # подписка
            F.col("event.subscription_channel").alias("subscription_channel"),
            F.col("event.user").cast("long").alias("subscription_user"),

            # время
            F.coalesce(
                F.col("event.message_ts"),
                F.col("event.datetime")
            ).alias("ts"),

            # координаты
            F.col("lat").alias("event_lat"),
            F.col("lon").alias("event_lon"),

            # тип события
            F.col("event_type").alias("event_type")
        )

        events = events.filter(F.col("user_id").isNotNull())

        cities = broadcast(
            self.cities_df.select(
                F.col("city"),
                F.col("lat").alias("city_lat"),
                F.col("lng").alias("city_lon")
            )
        )

        joined = events.crossJoin(cities)

        with_distance = joined.withColumn(
            "distance",
            Haversine.distance(
                F.col("event_lat"),
                F.col("event_lon"),
                F.col("city_lat"),
                F.col("city_lon")
            )
        )

        window = Window.partitionBy(
            "user_id",
            "message_id"
        ).orderBy("distance")

        result = (
            with_distance
            .withColumn("rn", F.row_number().over(window))
            .filter(F.col("rn") == 1)
        )

        return result.select(
            "user_id",
            "message_from",
            "message_to",
            "message_id",
            "subscription_channel",
            "subscription_user",
            "ts",
            "event_lat",
            "event_lon",
            "city",
            "distance",
            "event_type"
        )
