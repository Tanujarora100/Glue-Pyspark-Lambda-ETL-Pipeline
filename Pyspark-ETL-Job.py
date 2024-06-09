import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1717908597341 = glueContext.create_dynamic_frame.from_catalog(database="transaction_log", table_name="bankingrawtransactions", transformation_ctx="AWSGlueDataCatalog_node1717908597341")

# Script generated for node Drop Duplicates
DropDuplicates_node1717908617088 =  DynamicFrame.fromDF(AWSGlueDataCatalog_node1717908597341.toDF().dropDuplicates(["transaction_id"]), glueContext, "DropDuplicates_node1717908617088")

# Script generated for node Drop Null Fields
DropNullFields_node1717908997899 = drop_nulls(glueContext, frame=DropDuplicates_node1717908617088, nullStringSet={"", "null"}, nullIntegerSet={}, transformation_ctx="DropNullFields_node1717908997899")

# Script generated for node Aggregate
Aggregate_node1717908840327 = sparkAggregate(glueContext, parentFrame = DropNullFields_node1717908997899, groups = ["transaction_date", "transaction_type"], aggs = [["amount", "avg"]], transformation_ctx = "Aggregate_node1717908840327")

# Script generated for node Aggregate
Aggregate_node1717908793918 = sparkAggregate(glueContext, parentFrame = DropNullFields_node1717908997899, groups = ["description"], aggs = [["amount", "sum"]], transformation_ctx = "Aggregate_node1717908793918")

# Script generated for node Amazon S3
AmazonS3_node1717909288816 = glueContext.write_dynamic_frame.from_options(frame=Aggregate_node1717908840327, connection_type="s3", format="glueparquet", connection_options={"path": "s3://telecomglue/goldlayer/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1717909288816")

# Script generated for node Amazon S3
AmazonS3_node1717909467415 = glueContext.write_dynamic_frame.from_options(frame=Aggregate_node1717908793918, connection_type="s3", format="glueparquet", connection_options={"path": "s3://telecomglue/goldlayer/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1717909467415")

job.commit()