mport sys
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

# Script generated for node Customer Landing
CustomerLanding_node1707249727877 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-project",
    table_name="customer_landing",
    transformation_ctx="CustomerLanding_node1707249727877",
)

# Script generated for node Share With Research
SqlQuery0 = """
select * from customer_landing
WHERE shareWithResearchAsOfDate is not null;
"""
ShareWithResearch_node1707255657552 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": CustomerLanding_node1707249727877},
    transformation_ctx="ShareWithResearch_node1707255657552",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1706577136745 = glueContext.getSink(
    path="s3://stedi-analytics-lake/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1706577136745",
)
CustomerTrusted_node1706577136745.setCatalogInfo(
    catalogDatabase="stedi-project", catalogTableName="customer_trusted"
)
CustomerTrusted_node1706577136745.setFormat("json")
CustomerTrusted_node1706577136745.writeFrame(ShareWithResearch_node1707255657552)
job.commit()
