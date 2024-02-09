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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1707352319823 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1707352319823",
)

# Script generated for node Customer Curated
CustomerCurated_node1707352318353 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1707352318353",
)

# Script generated for node Join SQL Query
SqlQuery0 = """
select * from customer_curated join step_trainer_landing on customer_curated.serialNumber = step_trainer_landing.serialNumber;
"""
JoinSQLQuery_node1707352324954 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node1707352319823,
        "customer_curated": CustomerCurated_node1707352318353,
    },
    transformation_ctx="JoinSQLQuery_node1707352324954",
)

# Script generated for node Drop Fields
DropFields_node1707352353055 = DropFields.apply(
    frame=JoinSQLQuery_node1707352324954,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1707352353055",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1707352355251 = DynamicFrame.fromDF(
    DropFields_node1707352353055.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1707352355251",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1707352362087 = glueContext.getSink(
    path="s3://stedi-analytics-lake/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1707352362087",
)
StepTrainerTrusted_node1707352362087.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1707352362087.setFormat("json")
StepTrainerTrusted_node1707352362087.writeFrame(DropDuplicates_node1707352355251)
job.commit()
