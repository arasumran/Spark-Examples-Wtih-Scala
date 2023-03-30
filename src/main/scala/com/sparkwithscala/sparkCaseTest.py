
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_list, array_distinct,udf,split,input_file_name

os.environ['JAVA_HOME'] = "/usr/local/opt/openjdk@11"
folder_path = "data-engineering-coding-challenge/dataset"

# on production will change
def create_spark_session():
    return SparkSession.builder.master("local[1]") \
        .appName('SaprkReadExample') \
        .getOrCreate()

def read_file_with_spark(spark):
    tm =  spark.read.csv(folder_path + "/" + "*")
    return tm.withColumn("filename", input_file_name())

def extract_filename(some_string):
    return some_string.split("/")[1:][-1]

def split_string(val):
    return list(set(val.split(" "))) if val is not None else ''

if __name__ == '__main__':
    spark = create_spark_session()
    file_in = read_file_with_spark(spark=spark)
    udf_extract_filename = udf(extract_filename)

    # extract file name from path of file
    udf_split_string = udf(split_string)

    # splitting keyword in list and some internal arraging
    current_df = file_in.select(split(file_in._c0, ' ',0).alias('valueArray'),
                                udf_extract_filename("filename").alias("fileName")).drop("_c0")

    # explode every word and group every word by key collect into list files
    without_distinct = current_df.select(explode("valueArray").alias("key"), "fileName").groupBy('key').agg(
        collect_list("fileName").alias('files'))
    # delete repeated file name in list
    final = without_distinct.select('key', array_distinct(without_distinct.files).alias('files'))

    # show result
    print(final.where(final.key == 'Shakespeare').collect())











