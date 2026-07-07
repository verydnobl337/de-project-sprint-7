from pyspark.sql import functions as F
from pyspark.sql.window import Window


class HomeCityBuilder:
    MIN_DAYS = 27

    def __init__(self, city_matched_df):
        self.df = city_matched_df

    def build(self):

        df = self.df.select(
            'user_id',
            'city',
            F.to_date(F.col('ts')).alias('dt')
        ).dropDuplicates(["user_id", "city", "dt"])

        df = df.withColumn(
            "dt_int",
            F.datediff(F.col("dt"), F.lit("1970-01-01"))
        )

        window = Window.partitionBy("user_id", "city").orderBy("dt_int")

        df = df.withColumn(
            "rn",
            F.row_number().over(window)
        ).withColumn(
            "grp",
            F.col("dt_int") - F.col("rn")
        )

        grouped = (
            df.groupBy("user_id", "city", "grp")
            .agg(
                F.count("*").alias("days_count"),
                F.min("dt").alias("start_date"),
                F.max("dt").alias("end_date")
            )
        )

        valid = grouped.filter(F.col("days_count") >= self.MIN_DAYS)

        window2 = Window.partitionBy("user_id").orderBy(F.col("end_date").desc())

        result = (
            valid.withColumn("rn", F.row_number().over(window2))
            .filter(F.col("rn") == 1)
            .select(
                "user_id",
                F.col("city").alias("home_city")
            )
        )

        return result