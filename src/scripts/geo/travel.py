from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Создаю класс путешествий пользователя
class TravelBuilder:
    def __init__(self, city_matched_df):
        self.df = city_matched_df

    def build(self):
        # привожу данные к базовому виду:
        df = self.df.select(
            "user_id",
            "city",
            "ts",
            "timezone"
        )

        # сортирую по времени
        window = Window.partitionBy("user_id").orderBy("ts")

        df = df.withColumn("rn", F.row_number().over(window))

        # travel_array
        travel_array = (
            df.groupBy("user_id")
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
            df.groupBy("user_id")
            .agg(F.count("*").alias("travel_count"))
        )

        # local_time
        last_event = (
            df.withColumn(
                "local_time",
                F.from_utc_timestamp(F.col("ts"), F.col("timezone"))
            )
            .withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy("user_id").orderBy(F.col("ts").desc())
                )
            )
            .filter(F.col("rn") == 1)
            .select("user_id", "local_time")
        )

        # объединяю
        result = (
            travel_count
            .join(travel_array, "user_id", "left")
            .join(last_event, "user_id", "left")
        )

        return result