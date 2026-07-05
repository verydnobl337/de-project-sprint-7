from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast 

# В Jupyter без geo
from geo.haversine import Haversine


# Создаю класс для привязки событий к ближайшему городу
class CityMatcher:
    def __init__(self, events_df, cities_df):
        self.events_df = events_df
        self.cities_df = cities_df

    def match(self):
        """
        Логика:
        1) соединяю события и города
        2) считаю расстояние
        3) выбираю ближайший город
        """
        events = self.events_df.select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_id").alias("message_id"),
            F.col("event.message_ts").alias("ts"),
            F.col("lat").alias("event_lat"),
            F.col("lon").alias("event_lon")
        )
        # убираю шафл
        cities = broadcast(
            self.cities_df.select(
                F.col("city"),
                F.col("lat").alias("city_lat"),
                F.col("lng").alias("city_lon")
            )
        )
        # соединяю каждое событие с каждым городом
        joined = events.crossJoin(cities)
        # считаю расстояние
        with_distance = joined.withColumn(
            "distance",
            Haversine.distance(
                F.col("event_lat"),
                F.col("event_lon"),
                F.col("city_lat"),
                F.col("city_lon")
            )
        )
        # выбираю ближайший город для каждого события 
        window = Window.partitionBy('user_id', 'message_id').orderBy(F.col('distance'))
        
        result = with_distance.withColumn(
            'rn',
            F.row_number().over(window)
        ).filter(F.col('rn') == 1)

        # результат
        return result.select(
            "user_id",
            "message_id",
            "ts",
            "city",
            "distance"
        )