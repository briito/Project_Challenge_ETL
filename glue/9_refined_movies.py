import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
gc = GlueContext(sc)
spark = gc.spark_session
job = Job(gc)
job.init(args["JOB_NAME"], args)

s3_input = "s3://raw-briito/TRUSTED/MOVIES"
df = spark.read.option("multiline", "true").parquet(s3_input)

fact_movie_df = df.select(
    col("id").alias("movie"),
    explode(col("genres")).alias("genre")
).select(
    col("genre.id").alias("genre_id"),
    col("movie").alias("movie_id")
)

dim_movie_df = df.select(
    col("id").alias("id_movie"),
    col("title").alias("titulo"),
    col("runtime").alias("duracao"),
    col("adult").alias("adulto"),
    col("vote_average").alias("nota_media"),
    col("popularity").alias("popularidade"),
    col("release_date").alias("data_lancamento"),
    col("video").alias("trailer")
)

dim_genre_df = df.select(
    explode(col("genres")).alias("genre")
).select(
    col("genre.id").alias("genre_id"),
    col("genre.name").alias("genre_name")
)

fact_movie_dyF = DynamicFrame.fromDF(fact_movie_df, gc, "fact_movie")
dim_movie_dyF = DynamicFrame.fromDF(dim_movie_df, gc, "dim_movie")
dim_genre_dyF = DynamicFrame.fromDF(dim_genre_df, gc, "dim_genre")

output_path = "s3://raw-briito/REFINED/MOVIES"

gc.write_dynamic_frame.from_options(
    frame=fact_movie_dyF,
    connection_type="s3",
    connection_options={"path": f"{output_path}/fact_movie"},
    format="parquet"
)

gc.write_dynamic_frame.from_options(
    frame=dim_movie_dyF,
    connection_type="s3",
    connection_options={"path": f"{output_path}/dim_movie"},
    format="parquet"
)

gc.write_dynamic_frame.from_options(
    frame=dim_genre_dyF,
    connection_type="s3",
    connection_options={"path": f"{output_path}/dim_genre"},
    format="parquet"
)

job.commit()
