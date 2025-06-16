from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create SparkSession
spark = SparkSession.builder.appName("Employee Salary Analysis").getOrCreate()

# Define schema for the CSV file
schema = StructType([
    StructField("employee_id", StringType(), True),
    StructField("joining_date", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", StringType(), True)
])

# Read the employee CSV file with schema validation
df = spark.read.option("header", True).schema(schema).csv("employee_data.csv")

# Filter employees who joined after 2020-01-01
filtered_df = df.filter(df["joining_date"] > "2020-01-01")

# Cast salary column to integer
df_with_salary = filtered_df.withColumn("salary", col("salary").cast("int"))

# Calculate average salary per department
grouped_df = df_with_salary.groupBy("department").agg(avg("salary").alias("avg_salary"))

# Sort by average salary in descending order
sorted_df = grouped_df.sort("avg_salary", ascending=False)

# Select top 3 departments
top3_df = sorted_df.limit(3)

# Write result to ORC
top3_df.write.orc("output/top_departments.orc")
