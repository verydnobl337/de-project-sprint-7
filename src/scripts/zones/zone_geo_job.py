import sys
from pyspark.sql import SparkSession

from city_matcher import CityMatcher
from zone_event import ZoneEventsBuilder
from zone_mart import ZoneMartBuilder


def main():
    date = sys.argv[1]
    events_path = sys.argv[2]
    geo_path = sys.argv[3]
    output_path = sys.argv[4]

    spark = SparkSession.builder.appName(f"ZoneGeoJob-{date}").getOrCreate()

    events = spark.read.parquet(f"{events_path}/date={date}")
    geo = spark.read.option("header", True).option("sep", ";").csv(geo_path)

    city_matched = CityMatcher(events, geo).match()

    zone_events = ZoneEventsBuilder(city_matched).build()

    zone_mart = ZoneMartBuilder(zone_events).build()

    zone_mart.write.mode("overwrite").parquet(
        f"{output_path}/zones/date={date}"
    )


if __name__ == "__main__":
    main()