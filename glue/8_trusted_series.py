from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
gc = GlueContext(sc)
spark = gc.spark_session
job = Job(gc)

s3_input = "s3://raw-briito/RAW/TMDB/JSON/2023/10/SERIES/"

json_data = spark.read.option("multiline", "true").json(s3_input)

parquet_output = f"s3://raw-briito/TRUSTED/SERIES"

json_data.write.parquet(parquet_output, mode="overwrite")

job.commit()
