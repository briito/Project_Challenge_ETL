import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
gc = GlueContext(sc)
spark = SparkSession(sc)

job = Job(gc)
job.init(args["JOB_NAME"], args)

s3_input = "s3://raw-briito/TRUSTED/SERIES"
df_input = spark.read.option("multiline", "true").parquet(s3_input)

df_serie = df_input.select(
    explode(col("tv_results")).alias("tv_result")
)

df = df_serie.select(
    col("tv_result.id").alias("id_serie"),
    col("tv_result.name").alias("titulo"),
    col("tv_result.original_language").alias("idioma_original"),
    col("tv_result.popularity").alias("popularidade"),
    col("tv_result.vote_average").alias("nota_media"),
    col("tv_result.vote_count").alias("qtd_media"),
    col("tv_result.first_air_date").alias("data_lancamento")
)

output_path = "s3://raw-briito/REFINED/SERIES/fact_serie"

df.write.parquet(output_path, mode="overwrite")

job.commit()
