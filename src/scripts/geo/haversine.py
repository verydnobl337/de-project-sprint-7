import pyspark.sql.functions as F


# Создаю класс для расчета расстояния между двумя точками на сфере 
class Haversine:
    R = 6371 # Радиус

    # Перевожу градусы в радианы
    @staticmethod
    def to_radians(col):
        return F.radians(col)

    # Расчитываю расстояния между 2-мя точками
    @staticmethod
    def distance(lat1, lon1, lat2, lon2):
        # перевод в радианы
        lat1 = F.radians(lat1)
        lon1 = F.radians(lon1)
        lat2 = F.radians(lat2)
        lon2 = F.radians(lon2)

        # разница координат
        diff_lat = lat2 - lat1
        diff_lon = lon2 - lon1 

        # формула
        a = (
            F.pow(F.sin(diff_lat / 2), 2)
            + F.cos(lat1)
            * F.cos(lat2)
            * F.pow(F.sin(diff_lon / 2), 2)
        )

        c = 2 * F.asin(F.sqrt(a))

        # итоговое расстояние
        return Haversine.R * c
    