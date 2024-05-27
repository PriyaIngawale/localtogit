#######################################################################################################
# author name : Priyanka Ingawale
# Create Date : 16-12-2023
# last update date : 28-12-2023
# Description : This script is used for generating bronze layer tables basedon input datasets
import time

#######################################################################################################3


from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
import argparse
from datetime import datetime, timedelta
from utils.common_utils import CommonUtils

if __name__ == '__main__':
#     initializing the spark session

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # print(spark)

    # create daaframe on input file
    parser = argparse.ArgumentParser()

    parser.add_argument("-s","--source_name",
                        default="route",
                        help="please provide source name/dateset name")

    parser.add_argument("-sbp","--schema_base_path",default=r"D:\Users\Priyanka\PycharmProjects\airlinemgmnsystem\pythonProject1\source_schema", help="please provide source name/dateset name")

    parser.add_argument("-l", "--layer",default="bronze",help="please provide layer name")

    parser.add_argument("-bp", "--base_path_for_good_output",default=r"D:\airline project\output",help="please provide source name/dateset name")

    args = parser.parse_args()

    # initializing argument variables
    source_name = args.source_name
    schema_base_path = args.schema_base_path
    layer = args.layer
    base_path_for_good_output = args.base_path_for_good_output
    # print(f"source_name : {source_name}")

# # Get current date using Python's datetime
    current_date = datetime.now().date()
    print(current_date)
    # # Define start and end dates as strings
    start_date_str = '15-12-2023'
    end_date_str = '18-12-2023'

    # Convert start and end dates to datetime objects
    start_date = datetime.strptime(start_date_str, '%d-%m-%Y').date()
    end_date = datetime.strptime(end_date_str, '%d-%m-%Y').date()

    # Calculate difference in days using timedelta
    date_difference = (current_date - start_date).days
    date_difference1 = (current_date - end_date).days

    for i in range(date_difference, date_difference1 - 1, -1):


        # get yesterday date value
        yesterdays_date = datetime.now() - timedelta(days=i)
        print(yesterdays_date)
        day_before_yesterdays_date = yesterdays_date - timedelta(days=1)
        yesterdays_date = yesterdays_date.strftime("%Y%m%d")
        day_before_yesterdays_date = day_before_yesterdays_date.strftime("%Y%m%d")
        print(day_before_yesterdays_date)


        # initializing variables
        base_path = r"D:\airline project\input"
        input_file_path = fr"{base_path}\{source_name}\{yesterdays_date}"
        print(input_file_path)

        cmnutils = CommonUtils()

        # get or generate schema
        input_schema = cmnutils.generate_bronze_schema(schema_base_path, source_name)
        # print(input_schema)

        # create a dataframe on input file
        df = spark.read.csv(input_file_path, schema=input_schema)
        # print(df.printSchema())
        # df.show(100)

        if source_name=="airport":

            # iterate those column where 3rd colunm present
            input_schema_list = cmnutils.columns_to_check(schema_base_path,source_name)
            # print(input_schema_list)
            df = cmnutils.special_char_check(df,input_schema_list)
            # df.show()
            #
            cmnutils.datatype_conversion(df)
            #
            good_df = df.filter(df.special_char == '')
            # good_df.show()
            yesterdays_date_good_data = good_df.drop("special_char")
            yesterdays_date_good_data = yesterdays_date_good_data.withColumn("timestamp", current_timestamp())
            # # yesterdays_date_good_data.show()


            cmnutils.scd_type1(base_path_for_good_output, layer, source_name, day_before_yesterdays_date,yesterdays_date,yesterdays_date_good_data)


            bad_df = df.filter("special_char != ''")

            output_csv = fr"{input_file_path}\bad_df1_csv"
            df_2 = bad_df.write.mode("overwrite").csv(output_csv)
            bad_df_1 = spark.read.csv(output_csv)
            # bad_df_1.show()

        elif source_name=="route":
            input_column_list = cmnutils.columns_to_check(schema_base_path, source_name)
            # print(input_column_list)

            df = cmnutils.replace_null_check_with_empty1(df, input_column_list)
            # df.show(100)

            cmnutils.datatype_conversion(df)

            good_df = df.filter(df.null_check != 'Yes')
            # good_df.show()
            yesterdays_date_good_data = good_df.drop("null_check")

            cmnutils.good_data_for_route(base_path_for_good_output, layer, source_name, day_before_yesterdays_date,
                                         yesterdays_date, yesterdays_date_good_data)

            bad_df = df.filter(df.null_check == "")
            bad_output = fr"{input_file_path}\bad_df"
            bad_data = bad_df.write.mode("overwrite").csv(bad_output)

        elif source_name=="airline":
            input_column_list = cmnutils.columns_to_check(schema_base_path, source_name)
            # print(input_column_list)

            df = cmnutils.replace_null_check_with_empty(df, input_column_list)
            df = cmnutils.null_check(df, input_column_list)
            # df.show(300)

            cmnutils.datatype_conversion(df)

            df = df.filter(col("null_check") != "Yes")
            df.show()
            output_path = fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}'
            df_1 = df.write.mode("overwrite").parquet(output_path)

            # rename the file
            cmnutils.good_data_file_rename(base_path_for_good_output, layer, source_name, yesterdays_date)

            df = df.filter(col("null_check") == "")
            # # df.show()
            bad_output = fr"{input_file_path}\bad_df"
            df = df.write.mode("overwrite").csv(bad_output)
            break

        elif source_name=="plane":
            output_path = fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}'
            df = df.write.mode("overwrite").parquet(output_path)

            cmnutils.good_data_file_rename(base_path_for_good_output, layer, source_name, yesterdays_date)
            break


        else:
            print("Invalid date input")


