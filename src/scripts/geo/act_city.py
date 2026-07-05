from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Создаю класс актуального города пользователя 
class ActCityBuilder:

    def __init__(self, city_matched_df):
        self.df = city_matched_df

    def build(self):
                """
        Логика:
        1) берём все события пользователя с определённым городом
        2) находим последнее событие по времени
        3) считаем город этого события текущим местоположением пользователя
        """
        
        df = self.df.withColumn("ts", F.to_timestamp("ts"))
    
        # последнее смс пользователя
        window = Window.partitionBy('user_id').orderBy(F.col('ts').desc())

        df_with_rank = df.withColumn(
        'rn',
        F.row_number().over(window)
        )

        # оставляем последнее событие
        latest = df_with_rank.filter(F.col('rn') == 1)

        # формировка витрины 
        return latest.select(
            'user_id',
            F.col('city').alias('act_city'),
            'ts'
        )