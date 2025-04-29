import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, to_timestamp, when


spark = SparkSession.builder \
    .appName("AWS policies Analysis") \
    .getOrCreate()


file_path = os.path.expanduser("~/Downloads/Latest_IAM.csv")  


df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path)

# Clean column names (strip leading/trailing spaces)
df = df.select([col(c).alias(c.strip()) for c in df.columns])

# Debugging: Print the column names to ensure they are correct
print("Columns in DataFrame:", df.columns)

# Rename columns (ensure they match the actual column names)
df = df.withColumnRenamed("Last Used", "Last_Used")
df = df.withColumnRenamed("Creation Date", "Creation_Date")

# Convert "Last_Used" and "Creation_Date" to timestamp format
df = df.withColumn("Last_Used", to_timestamp(col("Last_Used"), "yyyy-MM-dd HH:mm:ssXXX"))
df = df.withColumn("Creation_Date", to_timestamp(col("Creation_Date"), "yyyy-MM-dd HH:mm:ssXXX"))

# Add a column for the number of days since last used
df = df.withColumn("Days Since Last Used", datediff(current_date(), col("Last_Used")))

# Add a column for the number of days since creation
df = df.withColumn("Days Since Creation", datediff(current_date(), col("Creation_Date")))

# Add an "Action" column based on the criteria
df = df.withColumn(
    "Action",
    when(
        (col("Days Since Last Used") > 90) & (col("Last_Used").isNull()), "Remove"  # Abandoned Roles
    ).when(
        (col("Days Since Last Used") > 180), "Remove"  # Stale Roles
    ).when(
        (~col("Role Name").rlike("^2FA-otp-trigger-workflow-")) | (col("Attached Policies").isNull()), "Review"  # Cleanup Candidates
    ).otherwise("Keep")  # Roles that don't meet any criteria
)

# Add a "Category" column to indicate which category the role falls under
df = df.withColumn(
    "Category",
    when(
        (col("Days Since Last Used") > 90) & (col("Last_Used").isNull()), "Abandoned Roles"
    ).when(
        (col("Days Since Last Used") > 180), "Stale Roles"
    ).when(
        (~col("Role Name").rlike("^2FA-otp-trigger-workflow-")) | (col("Attached Policies").isNull()), "Cleanup Candidates"
    ).otherwise("None")  # Roles that don't meet any criteria
)

# Show the final DataFrame with all columns
print("Final DataFrame with Actions and Categories:")
df.select(
    "Role Name", "Creation_Date", "Last_Used", "Days Since Last Used", 
    "Days Since Creation", "Attached Policies", "Action", "Category"
).show(truncate=False)

# Export the final DataFrame to a CSV file
output_path = os.path.expanduser("~/Downloads/role_analysis_results_002.csv")
df.write.option("header", "true").csv(output_path)

print(f"Results exported to: {output_path}")

# Stop Spark session
spark.stop()
