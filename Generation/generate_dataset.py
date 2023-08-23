#from hdfs.ext.kerberos import KerberosClient
import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, create_map
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import pyarrow.parquet as pq


def createDF() -> DataFrame:
    spark = SparkSession.builder.master("local").appName("parquetFile").getOrCreate()

    data = [("Basic", 500, ("480p", "No", "Yes")),
            ("Standard", 1500, ("720p", "Yes", "Yes")),
            ("Family Package", 3900, ("720p", "Yes", "No")),
            ("Premium", 7500, ("1080p", "Yes", "No")),
            ("Xtreme Premium", 6500, ("2K", "Yes", "No")),
            ("Supreme", 10000, ("2K", "Yes", "No")),
            ("Pay-As-Go", 710, ("480p", "No", "Yes")),
            ("Fixed Access", 2250, ("720p", "Yes", "No")),
            ("Enterprise", 11750, ("2K", "Yes", "No")),
            ("Supreme Plus", 12500, ("2k", "Yes", "No"))]

    schema = StructType([
        StructField('subscription', StringType(), True),
        StructField('numberOfChannels', LongType(), True),
        StructField('properties', StructType([
            StructField('quality', StringType(), True),
            StructField('mobileDownload', StringType(), True),
            StructField('ads', StringType(), True)
        ]))
    ])

    dataf = spark.createDataFrame(data=data, schema=schema)
    dataf = dataf.withColumn("extras", create_map(
        lit("quality"), col("properties.quality"),
        lit("mobileDownload"), col("properties.mobileDownload"),
        lit("ads"), col("properties.ads"))).drop("properties")

    return dataf


# dataf.printSchema()
# dataf.show(truncate=False)


def write(df: DataFrame):
    """host = 'http:// 0.0.0.0'
    port = 9870
    my_client = KerberosClient(host + ":" + str(port))

    # let's hard code the directory where this will land in HDFS
    direct = '/user/subscriptions/'
    filename = 'enriched_dataset.parquet'

    # remember we received the DataFrame and the filename as parameters
    with my_client.write(direct + filename, encoding='utf-8') as writer:
        df.write.mode("overwrite").parquet(writer)"""
    df.coalesce(1).write.parquet('hdfs://localhost:9870/subscriptions/dataset')


def start():
    dataframe = createDF()
    write(dataframe)


if __name__ == '__main__':
    start()
