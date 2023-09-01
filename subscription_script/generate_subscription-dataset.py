from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, create_map
from pyspark.sql.types import StructType, StructField, StringType, LongType


def create_df() -> DataFrame:
    spark = SparkSession.builder.master("local").appName("parquetFile").getOrCreate()  # connect with spark session

    data = [("Basic", 500, ("480p", "No", "Yes")),  # subscription data
            ("Standard", 1500, ("720p", "Yes", "Yes")),
            ("Family Package", 3900, ("720p", "Yes", "No")),
            ("Premium", 7500, ("1080p", "Yes", "No")),
            ("Xtreme Premium", 6500, ("2K", "Yes", "No")),
            ("Supreme", 10000, ("2K", "Yes", "No")),
            ("Pay-As-Go", 710, ("480p", "No", "Yes")),
            ("Fixed Access", 2250, ("720p", "Yes", "No")),
            ("Enterprise", 11750, ("2K", "Yes", "No")),
            ("Supreme Plus", 12500, ("2k", "Yes", "No"))]

    schema = StructType([  # schema
        StructField('subscription', StringType(), True),
        StructField('numberOfChannels', LongType(), True),
        StructField('properties', StructType([
            StructField('quality', StringType(), True),
            StructField('mobileDownload', StringType(), True),
            StructField('ads', StringType(), True)
        ]))
    ])

    df = spark.createDataFrame(data=data, schema=schema)  # use spark to create dataframe
    df = df.withColumn("extras", create_map(  # create map data type for extras 
        lit("quality"), col("properties.quality"),
        lit("mobileDownload"), col("properties.mobileDownload"),
        lit("ads"), col("properties.ads"))).drop("properties")

    return df


def start():
    dataframe = create_df()  # get subscription dataframe
    dataframe.write.save('../Kafka_Subscriptions', format="parquet")  # save subscription dataframe as parquet file
    """client = storage.Client.from_service_account_json(json_credentials_path=path_to_private_key)

# The bucket on GCS in which to write the CSV file
bucket = client.bucket('test-bucket-skytowner')
# The name assigned to the CSV file on GCS
blob = bucket.blob('my_data.csv')
blob.upload_from_string(df.to_csv(), 'text/csv')"""


if __name__ == '__main__':
    start()

"""
     from pyspark.sql import SparkSession
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("SnapshotJob").getOrCreate()
    
    # Load data
    input_path = "gs://your-bucket/input-data"
    data_df = spark.read.format("csv").load(input_path)
    
    # Process data
    # ...
    
    # Save data as Parquet to 
    output_path = "gs://your-bucket/output-parquet"
    data_df.write.parquet(output_path)
    
    # Stop Spark session
    spark.stop()

 """
