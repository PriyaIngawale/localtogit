from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, IntegerType, StringType, StructType
from pyspark.sql.functions import col, lit, when, regexp_extract, row_number, length
import os
from pyspark.sql.window import Window

class CommonUtils:
    def __init__(self):
        self.spark = SparkSession.builder.master('local[*]').getOrCreate()

    schema_dict = {}
    def generate_bronze_schema(self, base_path, source_name):
        """
        this method will generate pyspark schema for bronze layer dataset
        : param base_path:input schema base path
        : param source_name: input source name
        :return: StuctType schema
        """

        # initializing variable
        struct_field_lst = []

        try:
            # reading input schema files
            with open(fr"{base_path}\{source_name}_schema.csv", "r") as file:

                # iterating over each element in input files

                for elements in file.readlines():
                    column_nm = elements.split(",")[0]
                    datatype = elements.split(",")[1]
                    # print(column_nm,datatype)


                    # generate struct field values
                    struct_field_lst.append(StructField(column_nm, StringType()))
                    self.schema_dict[column_nm] = datatype

                # print(struct_field_lst)

                # generate structType schema values
                input_schema = StructType(struct_field_lst)
                # print(input_schema)

                return input_schema

        except Exception as e:
            raise Exception(f"ERROR : while running generate_bronze_schema method. Failed with exception : {e}")




    def datatype_conversion(self, df):
        for i in range(len(df.columns)-1):
            column = df.columns[i]
            df = df.withColumn(f"{column}", col(f"{column}").cast(self.schema_dict[column]))
        # print(df.printSchema())

    def columns_to_check(self,base_path, source_name):
        columns = []
        try:
            with open(fr'{base_path}\{source_name}_schema.csv', 'r') as r:
                for elements in r:
                    if 'special_char' in elements:
                        columns.append(elements.split(',')[0])

                    elif 'null_check' in elements:
                        columns.append(elements.split(',')[0])
        except Exception as e:
            raise Exception(f'Error while executing generate_bronze_schema method as {e}')
        return columns




    def special_char_check(self, df, column_ist):
        '''
        This method will retrieve special char from dataframe
        :param df: input dataframe
        :param col_name: column name on which special character check need to apply
        :return: dataframe
        '''

        try:
            special_char_check = r"([^A-Za-z0-9\s*])"
            df = df.withColumn("special_char", lit(""))
            for col_name in column_ist:
                # print(col_name)
                df = df.withColumn("special_char", when(col("special_char") == '',
                                    regexp_extract(f"{col_name}", special_char_check,1)).otherwise(
                                    col("special_char")))
                # df.select("airport_id","special_char").show()
        except Exception as e:
            raise Exception(f"ERROR: while running special_char_check method. Failed with execption: {e}")

        return df


    def good_data_file_rename(self, base_path_for_good_output, layer, source_name, yesterdays_date):
        try:
            list_of_files = os.listdir(fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}')
            for file in list_of_files:
                if file.endswith('.parquet'):
                    old_path = fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}\{file}'
                    new_path = fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}\{source_name}_{yesterdays_date}_good_data.parquet'
                    os.rename(old_path, new_path)
        except Exception as e:
            raise Exception(f'Error while executing generate_bronze_schema method as {e}')

    def scd_type1(self, base_path_for_good_output, layer, source_name, day_before_yesterdays_date,yesterdays_date,yesterdays_date_good_data):
        try:
            list_of_files = os.listdir(fr'{base_path_for_good_output}\{layer}\{source_name}\{day_before_yesterdays_date}')

            if len(list_of_files) == 0:
                yesterdays_date_good_data.write.parquet(fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}',mode='overwrite')

            for file in list_of_files:
                if file.endswith('.parquet'):
                    day_before_yesterdays_date_good_data = self.spark.read.parquet(
                        fr'{base_path_for_good_output}\{layer}\{source_name}\{day_before_yesterdays_date}', header=True)
                    # day_before_yesterdays_date_good_data.show()
                    # yesterdays_date_good_data.show()
                    final_good_data = day_before_yesterdays_date_good_data.union(yesterdays_date_good_data)
                    # final_good_data.show()
                    window_spec = Window.partitionBy('airport_id').orderBy(col('timestamp').desc())

                    final_good_data = final_good_data.withColumn('rownum', row_number().over(window_spec))
                    final_good_data = final_good_data.filter(col('rownum') == 1)
                    final_good_data = final_good_data.drop("rownum")

                    final_good_data.write.parquet(fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}',mode='overwrite')

                    # drop the ronum column
                    #  calling another function to rename the generated file
                    CommonUtils.good_data_file_rename(self, base_path_for_good_output, layer, source_name, yesterdays_date)
                else:
                    pass
        except FileNotFoundError as e:
            yesterdays_date_good_data.write.parquet(fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}',mode='overwrite')
            #  calling another function to rename the generated file
            CommonUtils.good_data_file_rename(self, base_path_for_good_output, layer, source_name, yesterdays_date)
#######################################################################################################################################



    def replace_null_check_with_empty(self,df,column_list):
        try:

            for col_name in column_list:
                df = df.withColumn(f"{col_name}",when(col(f"{col_name}").isNull(),"").when(col(f"{col_name}")==r"N/A","").when(col(f"{col_name}")== r"\N","").otherwise(col(f"{col_name}")))

        except Exception as e:
            raise Exception(f'Error while executing generate_bronze_schema method as {e}')
        return df

    def null_check(self,df,column_list):
        df = df.withColumn("null_check", lit(""))
        try:
            for col_name in column_list:
                df = df.withColumn("null_check",when(col(col_name) == "","Yes").otherwise(""))

                df = df.withColumn("airline_id",df.airline_id.cast(IntegerType()))
                df = df.withColumn("null_check",when(col("airline_id") < 0,"Yes").otherwise(col("null_check")))
                df = df.withColumn("null_check", when(length("iata") > 2, "Yes").otherwise(col("null_check")))
                df = df.withColumn("null_check", when(length("icao") > 3, "Yes").otherwise(col("null_check")))

                df = df.drop("null_check")


        except Exception as e:
            raise Exception(f'Error while executing columns_to_check_special_char method as {e}')
        return df


    def good_data_for_route(self, base_path_for_good_output, layer, source_name, day_before_yesterdays_date,yesterdays_date,yesterdays_date_good_data):
        try:
            list_of_files = os.listdir(fr'{base_path_for_good_output}\{layer}\{source_name}\{day_before_yesterdays_date}')

            if len(list_of_files) == 0:
                yesterdays_date_good_data.write.parquet(fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}',mode='overwrite')

            for file in list_of_files:
                if file.endswith('.parquet'):
                    day_before_yesterdays_date_good_data = self.spark.read.parquet(
                        fr'{base_path_for_good_output}\{layer}\{source_name}\{day_before_yesterdays_date}', header=True)

                    final_good_data = day_before_yesterdays_date_good_data.union(yesterdays_date_good_data)


                    final_good_data.coalesce(1).write.parquet(fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}',mode='overwrite')

                    # drop the ronum column
                    #  calling another function to rename the generated file
                    CommonUtils.good_data_file_rename(self, base_path_for_good_output, layer, source_name, yesterdays_date)
                else:
                    pass
        except FileNotFoundError as e:
            yesterdays_date_good_data.coalesce(1).write.parquet(fr'{base_path_for_good_output}\{layer}\{source_name}\{yesterdays_date}',mode='overwrite')
            #  calling another function to rename the generated file
            CommonUtils.good_data_file_rename(self, base_path_for_good_output, layer, source_name, yesterdays_date)

    def replace_null_check_with_empty1(self,df,column_list):
        try:
            df = df.withColumn('null_check', lit(''))

            for col_name in column_list:
                df = df.withColumn(f"{col_name}",when(col(f"{col_name}").isNull(),"").when(col(f"{col_name}")==r"N/A","").when(col(f"{col_name}")== r"\N","").otherwise(col(f"{col_name}")))

                # df = df.withColumn('null_check', lit(""))

                df = df.withColumn("null_check",when(col(col_name) == "","Yes").otherwise(col('null_check')))

                # df = df.drop(col("null_check"))
        except Exception as e:
            raise Exception(f'Error while executing generate_bronze_schema method as {e}')
        return df

##############################################################################################################################


