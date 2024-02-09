import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node Customer Trusted
CustomerTrusted_node1707346420427 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1707346420427",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707346421580 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1707346421580",
)

# Script generated for node Join SQL Query
SqlQuery0 = """
select * from customer_trusted join accelerometer_trusted on customer_trusted.email = accelerometer_trusted.user;
"""
JoinSQLQuery_node1707346427447 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": AccelerometerTrusted_node1707346421580,
        "customer_trusted": CustomerTrusted_node1707346420427,
    },
    transformation_ctx="JoinSQLQuery_node1707346427447",
)

# Script generated for node Drop Fields
DropFields_node1707346435762 = DropFields.apply(
    frame=JoinSQLQuery_node1707346427447,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1707346435762",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1707346438962 = DynamicFrame.fromDF(
    DropFields_node1707346435762.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1707346438962",
)

# Script generated for node Customer Curated
CustomerCurated_node1707346443466 = glueContext.getSink(
    path="s3://stedi-analytics-lake/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1707346443466",
)
CustomerCurated_node1707346443466.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="customer_curated"
)
CustomerCurated_node1707346443466.setFormat("json")
CustomerCurated_node1707346443466.writeFrame(DropDuplicates_node1707346438962)
job.commit()
