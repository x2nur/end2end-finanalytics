import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_derived

args = getResolvedOptions(sys.argv, ["JOB_NAME", "src_cards_s3", "dest_cards_s3"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

src_cards = args["src_cards_s3"]
dest_cards = args["dest_cards_s3"]

# Script generated for node Raw cards data
Rawcardsdata_node1774948245567 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": [src_cards], "recurse": True},
    transformation_ctx="Rawcardsdata_node1774948245567",
)

# Script generated for node Change Schema
ChangeSchema_node1774950043020 = ApplyMapping.apply(
    frame=Rawcardsdata_node1774948245567,
    mappings=[
        ("id", "string", "id", "int"),
        ("client_id", "string", "client_id", "int"),
        ("card_brand", "string", "card_brand", "string"),
        ("card_type", "string", "card_type", "string"),
        ("card_number", "string", "card_number", "string"),
        ("expires", "string", "expires", "string"),
        ("cvv", "string", "cvv", "int"),
        ("has_chip", "string", "has_chip", "string"),
        ("num_cards_issued", "string", "num_cards_issued", "int"),
        ("credit_limit", "string", "credit_limit", "string"),
        ("acct_open_date", "string", "acct_open_date", "string"),
        ("year_pin_last_changed", "string", "year_pin_last_changed", "int"),
        ("card_on_dark_web", "string", "card_on_dark_web", "string"),
    ],
    transformation_ctx="ChangeSchema_node1774950043020",
)

# Script generated for node Replace $ in credit_limit
Replaceincredit_limit_node1774950526857 = ChangeSchema_node1774950043020.gs_derived(
    colName="credit_limit", expr="cast(replace(credit_limit, '$', '') as decimal(10,2))"
)

# Script generated for node Add expire year
Addexpireyear_node1774952460591 = Replaceincredit_limit_node1774950526857.gs_derived(
    colName="expire_year", expr="cast(split(expires, '/')[1] as int)"
)

# Script generated for node Add expire month
Addexpiremonth_node1774952535846 = Addexpireyear_node1774952460591.gs_derived(
    colName="expire_month", expr="cast(split(expires, '/')[0] as int)"
)

# Script generated for node Add acct open year
Addacctopenyear_node1774952636609 = Addexpiremonth_node1774952535846.gs_derived(
    colName="acct_open_year", expr="cast(split(acct_open_date, '/')[1] as int)"
)

# Script generated for node Add acct open month
Addacctopenmonth_node1774952711132 = Addacctopenyear_node1774952636609.gs_derived(
    colName="acct_open_month", expr="cast(split(acct_open_date, '/')[0] as int)"
)

# Script generated for node Drop expires, acct_open_date
Dropexpiresacct_open_date_node1774952775685 = DropFields.apply(
    frame=Addacctopenmonth_node1774952711132,
    paths=["acct_open_date", "expires"],
    transformation_ctx="Dropexpiresacct_open_date_node1774952775685",
)

# Script generated for node Cards result
Cardsresult_node1776517873478 = glueContext.write_dynamic_frame.from_options(
    frame=Dropexpiresacct_open_date_node1774952775685,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": dest_cards, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="Cardsresult_node1776517873478",
)

job.commit()
