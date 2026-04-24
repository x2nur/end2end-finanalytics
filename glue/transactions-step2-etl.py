import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import gs_derived

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "src_tx_data_s3", "dest_tx_data_s3", "src_zipcodes_data_s3"]
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

src_data = args["src_tx_data_s3"]
dest_data = args["dest_tx_data_s3"]
src_zipcodes_data = args["src_zipcodes_data_s3"]

# Script generated for node Missing zip codes
Missingzipcodes_node1774635506978 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": [src_zipcodes_data], "recurse": True},
    transformation_ctx="Missingzipcodes_node1774635506978",
)

# Script generated for node Tx - step 1
Txstep1_node1774628115942 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [src_data], "recurse": True},
    transformation_ctx="Txstep1_node1774628115942",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1774636044339 = ApplyMapping.apply(
    frame=Missingzipcodes_node1774635506978,
    mappings=[
        ("city", "string", "right_city", "string"),
        ("country", "string", "right_country", "string"),
        ("zip", "string", "right_zip", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1774636044339",
)

# Script generated for node Join
Txstep1_node1774628115942DF = Txstep1_node1774628115942.toDF()
RenamedkeysforJoin_node1774636044339DF = RenamedkeysforJoin_node1774636044339.toDF()
Join_node1774635663062 = DynamicFrame.fromDF(
    Txstep1_node1774628115942DF.join(
        RenamedkeysforJoin_node1774636044339DF,
        (
            Txstep1_node1774628115942DF["merchant_city"]
            == RenamedkeysforJoin_node1774636044339DF["right_city"]
        )
        & (
            Txstep1_node1774628115942DF["merchant_state"]
            == RenamedkeysforJoin_node1774636044339DF["right_country"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1774635663062",
)

# Script generated for node Coalesce zip
Coalescezip_node1774636214135 = Join_node1774635663062.gs_derived(
    colName="zip", expr="COALESCE(NULLIF(zip, ''), right_zip)"
)

# Script generated for node Fields w/o right table
Fieldsworighttable_node1774636155063 = SelectFields.apply(
    frame=Coalescezip_node1774636214135,
    paths=[
        "id",
        "event_date",
        "client_id",
        "card_id",
        "amount",
        "use_chip",
        "merchant_id",
        "merchant_city",
        "merchant_state",
        "zip",
        "mcc",
        "errors",
    ],
    transformation_ctx="Fieldsworighttable_node1774636155063",
)

# Script generated for node Final tx result
Finaltxresult_node1776329740453 = glueContext.write_dynamic_frame.from_options(
    frame=Fieldsworighttable_node1774636155063,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": dest_data, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="Finaltxresult_node1776329740453",
)

job.commit()
