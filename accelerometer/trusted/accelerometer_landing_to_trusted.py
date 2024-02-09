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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1707268557297 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1707268557297",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1707268860950 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1707268860950",
)

# Script generated for node Join SQL Query
SqlQuery0 = """
select * from customer_trusted join accelerometer_landing on customer_trusted.email = accelerometer_landing.user;
"""
JoinSQLQuery_node1707342676169 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1707268557297,
        "customer_trusted": CustomerTrusted_node1707268860950,
    },
    transformation_ctx="JoinSQLQuery_node1707342676169",
)

# Script generated for node Drop Fields
DropFields_node1707345045292 = DropFields.apply(
    frame=JoinSQLQuery_node1707342676169,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1707345045292",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707268608956 = glueContext.getSink(
    path="s3://stedi-analytics-lake/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1707268608956",
)
AccelerometerTrusted_node1707268608956.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1707268608956.setFormat("json")
AccelerometerTrusted_node1707268608956.writeFrame(DropFields_node1707345045292)
job.commit()
