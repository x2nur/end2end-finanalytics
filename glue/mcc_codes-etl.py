import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "src_mcc_codes_s3", "dest_mcc_codes_s3"]
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

src_mcc_codes = args["src_mcc_codes_s3"]
dest_mcc_codes = args["dest_mcc_codes_s3"]

# Script generated for node Raw mcc codes
Rawmcccodes_node1774966208032 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": [src_mcc_codes], "recurse": True},
    transformation_ctx="Rawmcccodes_node1774966208032",
)

# Script generated for node Change Schema
ChangeSchema_node1775043036296 = ApplyMapping.apply(
    frame=Rawmcccodes_node1774966208032,
    mappings=[
        ("mcc", "string", "mcc", "int"),
        ("desc", "string", "description", "string"),
    ],
    transformation_ctx="ChangeSchema_node1775043036296",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1774967262663 = DynamicFrame.fromDF(
    ChangeSchema_node1775043036296.toDF().dropDuplicates(["mcc"]),
    glueContext,
    "DropDuplicates_node1774967262663",
)

# Script generated for node MccCodes result
MccCodesresult_node1776749090857 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1774967262663,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": dest_mcc_codes, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="MccCodesresult_node1776749090857",
)

job.commit()
