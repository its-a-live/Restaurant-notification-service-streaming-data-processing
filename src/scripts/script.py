from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
from pyspark.sql import functions as f
from datetime import datetime
import options as o
from time import sleep

def spark_init(test_name) -> SparkSession:
    spark = (
        SparkSession.builder.appName(test_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", o.spark_jars_packages)
            .getOrCreate()
    )
    return spark

def read_adv_stream(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", DoubleType()),
        StructField("adv_campaign_datetime_end", DoubleType()),
        StructField("datetime_created", DoubleType())
    ])

    df_adv = (spark.readStream
        .format('kafka')
        .options(**o.kafka_security_options)
        .option('subscribe', o.TOPIC_IN)
        .load()
        .withColumn('value', f.col('value').cast(StringType()))
        .withColumn('advert', f.from_json(f.col('value'), schema))
        .selectExpr('advert.*')
        .where((f.col("adv_campaign_datetime_start") < f.unix_timestamp(f.current_timestamp()))
        & (f.col("adv_campaign_datetime_end") > f.unix_timestamp(f.current_timestamp()))
    )
    return df_adv

def read_user(spark: SparkSession) -> DataFrame:
    df_user = (spark.read
                    .format("jdbc")
                    .option("url", "jdbc:postgresql://?cloud.net:6432/")
                    .option("dbtable", "subscribers_restaurants")
                    .option("driver", "org.postgresql.Driver")
                    .options(**o.psql_settings)
                    .load()
    )
    return df_user

def join(df_adv, df_user) -> DataFrame:
    join_df = df_adv \
    .join(df_user, 'restaurant_id') \
    .withColumn('trigger_datetime_created', f.unix_timestamp(f.current_timestamp())) \
    .select(
        'restaurant_id',
        'adv_campaign_id',
        'adv_campaign_content',
        'adv_campaign_owner',
        'adv_campaign_owner_contact',
        'adv_campaign_datetime_start',
        'adv_campaign_datetime_end',
        'datetime_created',
        'client_id',
        'trigger_datetime_created'
    )
    return join_df

def foreach_batch_function(df, epoch_id):
    df.persist()

    feedback_df = df.withColumn('feedback', f.lit(None).cast(StringType()))

    feedback_df.write.format('jdbc').mode('append') \
        .options(**o.psql_settings_for_docker).save()

    df_to_stream = (feedback_df
                .select(f.to_json(f.struct(
                    'restaurant_id',
                    'adv_campaign_id',
                    'adv_campaign_content',
                    'adv_campaign_owner',
                    'adv_campaign_owner_contact',
                    'adv_campaign_datetime_start',
                    'adv_campaign_datetime_end',
                    'datetime_created',
                    'client_id',
                    'trigger_datetime_created'
                )).alias('value'))
                .select('value')
                )

    df_to_stream.write \
        .format('kafka') \
        .options(**o.kafka_security_options) \
        .option('topic', o.TOPIC_OUT) \
        .save()

    df.unpersist()

if __name__ == "__main__":
    spark = spark_init('adv_Restaurant_campaign_for_user')
    adv_stream = read_adv_stream(spark)
    user_df = read_user(spark)
    output = join(adv_stream, user_df)

    query = (output.writeStream
              .foreachBatch(foreach_batch_function)
              .option("checkpointLocation", "checkpoints")
              .start()
              .awaitTermination())