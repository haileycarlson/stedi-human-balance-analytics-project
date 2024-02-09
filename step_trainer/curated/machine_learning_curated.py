import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707353355965 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1707353355965",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707353354508 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1707353354508",
)

# Script generated for node Join SQL Query
SqlQuery0 = """
select * from accelerometer_trusted join step_trainer_trusted on accelerometer_trusted.timestamp = step_trainer_trusted.sensorReadingTime;
"""
JoinSQLQuery_node1707353366011 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1707353355965,
        "step_trainer_trusted": StepTrainerTrusted_node1707353354508,
    },
    transformation_ctx="JoinSQLQuery_node1707353366011",
)

# Script generated for node Drop Fields
DropFields_node1707353375985 = DropFields.apply(
    frame=JoinSQLQuery_node1707353366011,
    paths=["z", "y", "x", "timestamp", "user"],
    transformation_ctx="DropFields_node1707353375985",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1707353383572 = glueContext.getSink(
    path="s3://stedi-analytics-lake/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1707353383572",
)
MachineLearningCurated_node1707353383572.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1707353383572.setFormat("json")
MachineLearningCurated_node1707353383572.writeFrame(DropFields_node1707353375985)
job.commit()
