from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Создаю класс путешествий пользователя
class TravelBuilder:
    def __init__(self, city_matched_df):
        self.df = city_matched_df

    def build(self):
        # привожу данные к базовому виду
        df = self.df.select(
            "user_id",
            "city",
            F.to_timestamp("ts").alias("ts")
        )

        # сортировка событий пользователя по времени
        travel_window = Window.partitionBy("user_id").orderBy("ts")

        # оставляю только моменты смены города
        travels = (
            df.withColumn(
                "prev_city",
                F.lag("city").over(travel_window)
            )
            .filter(
                F.col("prev_city").isNull() |
                (F.col("city") != F.col("prev_city"))
            )
        )

        # travel_array
        travel_array = (
            travels.groupBy("user_id")
            .agg(
                F.collect_list(
                    F.struct("ts", "city")
                ).alias("arr")
            )
            .withColumn(
                "travel_array",
                F.expr("transform(array_sort(arr), x -> x.city)")
            )
            .select("user_id", "travel_array")
        )

        # travel_count
        travel_count = (
            travels.groupBy("user_id")
            .agg(
                F.count("*").alias("travel_count")
            )
        )

        # local_time
        last_event = (
            df.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("user_id").orderBy(F.col("ts").desc())
                )
            )
            .filter(F.col("rn") == 1)
            .select("user_id", "ts")
            .withColumnRenamed("ts", "local_time")
        )

        result = (
            travel_count
            .join(travel_array, "user_id", "left")
            .join(last_event, "user_id", "left")
        )

        return result