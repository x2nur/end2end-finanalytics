import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_repartition
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import gs_derived
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'src_data_s3', 'dest_data_s3', 'missing_zipcodes_s3'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


src_data = args['src_data_s3']
dest_data = args['dest_data_s3']
miss_zipcodes_data = args['missing_zipcodes_s3']


# Script generated for node Raw tx data 
Rawtxdata_node1774367096973 = (
    glueContext
    .create_dynamic_frame
    .from_options(
        format_options={
            "quoteChar": "\"", 
            "withHeader": True, 
            "separator": ",", 
            "optimizePerformance": False
        }, 
        connection_type="s3", 
        format="csv", 
        connection_options={"paths": [src_data]}, 
        transformation_ctx="Rawtxdata_node1774367096973"
)


# Script generated for node Change Schema
ChangeSchema_node1774374449055 = (
    ApplyMapping
    .apply(
        frame=Rawtxdata_node1774367096973, 
        mappings=[
            ("id", "string", "id", "int"), 
            ("date", "string", "event_date", "timestamp"), 
            ("client_id", "string", "client_id", "int"), 
            ("card_id", "string", "card_id", "int"), 
            ("amount", "string", "amount", "string"), 
            ("use_chip", "string", "use_chip", "string"), 
            ("merchant_id", "string", "merchant_id", "int"), 
            ("merchant_city", "string", "merchant_city", "string"), 
            ("merchant_state", "string", "merchant_state", "string"), 
            ("zip", "string", "zip", "string"), 
            ("mcc", "string", "mcc", "int"), 
            ("errors", "string", "errors", "string")
        ], 
        transformation_ctx="ChangeSchema_node1774374449055" 
    )
)

# Script generated for node Drop Duplicates
DropDuplicates_node1774376397670 = (
    DynamicFrame
    .fromDF(
        ChangeSchema_node1774374449055
        .toDF()
        .dropDuplicates(
            ["client_id", "card_id", "amount", "merchant_id", "event_date"]
        ), 
        glueContext, 
        "DropDuplicates_node1774376397670"
    )
)

# Script generated for node Amount col - replace $, decimal
Amountcolreplacedecimal_node1774371073286 = (
    DropDuplicates_node1774376397670
    .gs_derived(
        colName="amount", 
        expr="cast(replace(amount, '$', '') as decimal(10,2))"
    )
)

# Script generated for node MerchantState col - ONLINE
MerchantStatecolONLINE_node1774372571269 = (
    Amountcolreplacedecimal_node1774371073286
    .gs_derived(
        colName="merchant_state", 
        expr="CASE WHEN LOWER(merchant_city) = 'online' THEN 'ONLINE' ELSE merchant_state END"
    )
)

# Script generated for node Fill 'No error'
FillNoerror_node1774694539337 = (
    MerchantStatecolONLINE_node1774372571269
    .gs_derived(
        colName="errors", 
        expr="CASE WHEN errors IS NULL OR TRIM(errors) = '' THEN 'NO ERROR' ELSE errors END"
    )
)

# Script generated for node Zip col - N/A
ZipcolNA_node1774376178642 = (
    FillNoerror_node1774694539337
    .gs_derived(
        colName="zip", 
        expr="CASE WHEN LOWER(merchant_city) = 'online' THEN 'N/A' ELSE zip END"
    )
)

# Script generated for node Zip col is null
SqlQuery37 = '''
select * from myDataSource
where (zip is null 
or trim(zip) = '') 
and merchant_city is not null 
and merchant_state is not null
'''

Zipcolisnull_node1774627888348 = (
    sparkSqlQuery(
        glueContext, 
        query = SqlQuery37, 
        mapping = { "myDataSource": ZipcolNA_node1774376178642 }, 
        transformation_ctx = "Zipcolisnull_node1774627888348"
    )
)

# Script generated for node Merchant city, country
Merchantcitycountry_node1774377292518 = SelectFields.apply(frame=Zipcolisnull_node1774627888348, paths=["merchant_city", "merchant_state"], transformation_ctx="Merchantcitycountry_node1774377292518")

Changecitystatenames_node1776340653781 = ApplyMapping.apply(frame=Merchantcitycountry_node1774377292518, mappings=[("merchant_city", "string", "city", "string"), ("merchant_state", "string", "country", "string")], transformation_ctx="Changecitystatenames_node1776340653781")

# Script generated for node Drop city/country dups
Dropcitycountrydups_node1774378142239 =  DynamicFrame.fromDF(Changecitystatenames_node1776340653781.toDF().dropDuplicates(), glueContext, "Dropcitycountrydups_node1774378142239")

# Script generated for node Autobalance Processing
AutobalanceProcessing_node1776196873020 = Dropcitycountrydups_node1774378142239.gs_repartition(numPartitionsStr="1")

# Script generated for node Step 1 result
Step1result_node1774376690037 = glueContext.write_dynamic_frame.from_options(frame=ZipcolNA_node1774376178642, connection_type="s3", format="glueparquet", connection_options={"path": dest_data, "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Step1result_node1774376690037")

# Script generated for node Merchant w/ null zip
if (AutobalanceProcessing_node1776196873020.count() >= 1):
   AutobalanceProcessing_node1776196873020 = AutobalanceProcessing_node1776196873020.coalesce(1)
Merchantwnullzip_node1774377132771 = glueContext.write_dynamic_frame.from_options(frame=AutobalanceProcessing_node1776196873020, connection_type="s3", format="csv", connection_options={"path": miss_zipcodes_data, "partitionKeys": []}, transformation_ctx="Merchantwnullzip_node1774377132771")

job.commit()
