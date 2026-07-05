import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from city_matcher import CityMatcher
from act_city import ActCityBuilder
from home_city import HomeCityBuilder
from travel import TravelBuilder


def main():
    date = sys.argv[1]
    events_path = sys.argv[2]
    geo_path = sys.argv[3]
    output_path = sys.argv[4]

    spark = SparkSession.builder.appName(f"UserGeoJob-{date}").getOrCreate()

    events = spark.read.parquet(f"{events_path}/date={date}")
    geo = spark.read.option("header", True).option("sep", ";").csv(geo_path)

    city_matched = CityMatcher(events, geo).match()

    act_city = ActCityBuilder(city_matched).build()
    home_city = HomeCityBuilder(city_matched).build()
    travel = TravelBuilder(city_matched).build()

    result = (
        act_city
        .join(home_city, "user_id", "left")
        .join(travel, "user_id", "left")
    )

    result.write.mode("overwrite").parquet(f"{output_path}/users_geo/date={date}")


if __name__ == "__main__":
    main()