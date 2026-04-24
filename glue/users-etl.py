import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_derived

args = getResolvedOptions(sys.argv, ["JOB_NAME", "src_users_s3", "dest_users_s3"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

src_users = args["src_users_s3"]
dest_users = args["dest_users_s3"]

# Script generated for node Raw User data
RawUserdata_node1774879907922 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": [src_users], "recurse": True},
    transformation_ctx="RawUserdata_node1774879907922",
)

# Script generated for node Change Schema
ChangeSchema_node1774882481976 = ApplyMapping.apply(
    frame=RawUserdata_node1774879907922,
    mappings=[
        ("id", "string", "id", "int"),
        ("current_age", "string", "current_age", "int"),
        ("retirement_age", "string", "retirement_age", "int"),
        ("birth_year", "string", "birth_year", "int"),
        ("birth_month", "string", "birth_month", "int"),
        ("gender", "string", "gender", "string"),
        ("address", "string", "address", "string"),
        ("latitude", "string", "latitude", "float"),
        ("longitude", "string", "longitude", "float"),
        ("per_capita_income", "string", "per_capita_income", "string"),
        ("yearly_income", "string", "yearly_income", "string"),
        ("total_debt", "string", "total_debt", "string"),
        ("credit_score", "string", "credit_score", "int"),
        ("num_credit_cards", "string", "num_credit_cards", "int"),
    ],
    transformation_ctx="ChangeSchema_node1774882481976",
)

# Script generated for node Replace $ in per_capita_income | decimal
Replaceinper_capita_incomedecimal_node1774946086935 = (
    ChangeSchema_node1774882481976.gs_derived(
        colName="per_capita_income",
        expr="cast(replace(per_capita_income, '$', '') as decimal(10,2))",
    )
)

# Script generated for node Replace $ in yearly_income | decimal
Replaceinyearly_incomedecimal_node1774946165211 = (
    Replaceinper_capita_incomedecimal_node1774946086935.gs_derived(
        colName="yearly_income",
        expr="cast(replace(yearly_income, '$', '') as decimal(10,2))",
    )
)

# Script generated for node Replace $ in total_debt
Replaceintotal_debt_node1774946250067 = (
    Replaceinyearly_incomedecimal_node1774946165211.gs_derived(
        colName="total_debt", expr="cast(replace(total_debt, '$', '') as decimal(10,2))"
    )
)

# Script generated for node Recalc current_age
Recalccurrent_age_node1774946337065 = Replaceintotal_debt_node1774946250067.gs_derived(
    colName="current_age",
    expr="YEAR(CURRENT_DATE()) - birth_year - CASE WHEN MONTH(CURRENT_DATE()) - birth_month >=0 THEN 0 ELSE 1 END",
)

# Script generated for node is_retired
is_retired_node1774946584884 = Recalccurrent_age_node1774946337065.gs_derived(
    colName="is_retired", expr="current_age >= retirement_age"
)

# Script generated for node User results
Userresults_node1776511919141 = glueContext.write_dynamic_frame.from_options(
    frame=is_retired_node1774946584884,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": dest_users, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="Userresults_node1776511919141",
)

job.commit()
