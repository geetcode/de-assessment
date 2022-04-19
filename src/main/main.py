from pathlib import Path

from pyspark.sql import SparkSession
import sys
from urllib.parse import urlparse, parse_qsl, parse_qs
from pyspark.sql.functions import  explode_outer
from pyspark.sql import functions as f

from src.main.read import FileReader



def create_spark_session(master):
    #print("master", master)
    return SparkSession.builder \
        .master(master) \
        .appName("Search Data Processing") \
        .getOrCreate()


def get_product_lists(product_string):
    product_list_out = []
    if product_string is not None:
        product_array = product_string.split(",")
        for product in product_array:
            product_details = product.split(";")
            product_list_out.append(product_details)
        return product_list_out


def get_search_from_url(referrer):
    parts = referrer.split('?')
    result = {}
    if len(parts) > 1:
        q = parts[1]
        try:
            q = parse_qs(q)
        except Exception:
            q = parse_qs('')  # so it's still iterable
        result = dict((k, v[0]) for k, v in q.items())

    search_keyword = ''
    if result.get('q') is not None:
        search_keyword = result['q']
    elif result.get('p') is not None:
        search_keyword = result['p']
    #print(search_keyword)
    return search_keyword


def get_domain_from_url(referrer):
    domain = urlparse(referrer).netloc
    #print(domain)
    return domain


def mapToProductArray(x):
    #print(x.product_list)
    hit_time_gmt = x.hit_time_gmt
    date_time = x.date_time
    user_agent = x.user_agent
    ip = x.ip
    event_list = x.event_list
    geo_city = x.geo_city
    geo_region = x.geo_region
    geo_county = x.geo_country
    pagename = x.pagename
    page_url = x.page_url
    product_list = get_product_lists(x.product_list)
    referrer = x.referrer
    search_domain = get_domain_from_url(x.referrer)
    search_keyword = get_search_from_url(x.referrer)
    return (hit_time_gmt, date_time,
            user_agent, ip, event_list, geo_city,
            geo_region, geo_county, pagename, page_url, product_list,
            referrer,search_domain,search_keyword)


if __name__ == '__main__':
    spark = create_spark_session(sys.argv[1])
    reader = FileReader(spark)
    df = reader.read_txt_file(sys.argv[2], delimiter="\t")
    final_columns = [s.name for s in df.schema] + ['search_domain', 'search_keyword']

    df2 = df.rdd.map(lambda x: mapToProductArray(x)).toDF(final_columns)
    exploded_df = df2.withColumn("exploded_list", explode_outer(f.col("product_list"))).drop("product_list")

    final_df = exploded_df.withColumn("revenue", f.coalesce(f.col("exploded_list").getItem(3).cast("float"), f.lit(0.0))) \
        .groupBy("search_domain", "search_keyword") \
        .agg(f.sum("revenue").alias("sum_revenue")) \
        .orderBy(f.col("sum_revenue").desc())

    final_df.coalesce(1).\
        write.option("header", True).\
        mode('overwrite').\
        option("delimiter","\t").\
        csv("out/datacsv")
    #
    # fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    #
    # # list files in the directory
    # list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(sys.argv[3]))
    #
    # # filter name of the file starts with part-
    # file_name = [file.getPath().getName() for file in list_status if file.getPath().getName().startswith('part-')][0]
    #
    # # rename the file
    # fs.rename(Path("hdfs://out/datacsv/" + '' + file_name), Path("hdfs://out/datacsv/" + '' + "_SearchKeywordPerformance.tab"))