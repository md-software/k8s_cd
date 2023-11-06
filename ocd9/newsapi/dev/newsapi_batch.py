import os
import glob
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize a Spark session
spark = SparkSession.builder.appName("JSONAggregations").getOrCreate()

# Define the path to the directory containing the JSON files
json_data_path = "/tmp"

# List all JSON files in the directory
json_files = glob.glob(os.path.join(json_data_path, "batch_*.txt"))

print("#### number of json files: ", len(json_files))

try:
    # Read JSON files into a DataFrame
    json_df = spark.read.json(json_files)

    # Show the schema of the DataFrame
    json_df.printSchema()

    # Perform some data transformations and aggregations
    agg_result = json_df.groupBy("source.name").agg(
        count("title").alias("count")#,
        #sum("amount").alias("total_amount")
    )

    # Show the aggregated results in console
    agg_result.show()

    # Get the current datetime
    current_datetime = datetime.datetime.now()

    # Format the datetime as a string
    formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H:%M:%S")

    # Save the result of agg_result.show() to a csv file
    agg_result.write.mode("overwrite").format("csv").save(json_data_path+"/agg_newsapi_"+formatted_datetime)

except Exception as e:
    # Handle the exception here, e.g., log the error
    print(f"An error occurred: {str(e)}")

# Delete the processed files
for json_file in json_files:
    os.remove(json_file)

# Stop the Spark session
spark.stop()
