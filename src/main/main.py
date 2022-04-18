from pyspark.sql import SparkSession
import sys

from src.main.read import FileReader


def create_spark_session(master):
    print("master", master)
    return SparkSession.builder \
        .master(master) \
        .appName("Search Data Processing") \
        .getOrCreate()


if __name__ == '__main__':
    print(sys.argv)
    spark = create_spark_session(sys.argv[1])
    reader = FileReader(spark)
    df = reader.read_txt_file(sys.argv[2], delimiter="\t")
    print(df.count())
    df.printSchema()
