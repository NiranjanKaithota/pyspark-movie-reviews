from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length

# Initialize the Spark Session
spark = SparkSession.builder \
    .appName("Movie Reviews Processing") \
    .getOrCreate()

# 1. Load the Dataset
# Use PySpark to read the CSV file into a DataFrame[cite: 4].
df = spark.read.csv("IMDB Dataset.csv", header=True, inferSchema=True)

# 2. Explore the Data
# Print the schema of the DataFrame[cite: 4].
print("--- DataFrame Schema ---")
df.printSchema()

# Display the first 5 rows[cite: 4].
print("--- First 5 Rows ---")
df.show(5, truncate=False)

# 3. Clean the Data
# Remove any rows with null or empty reviews[cite: 4].
# dropna() removes nulls, filter() removes empty string arrays.
clean_df = df.dropna(subset=["review"]).filter(col("review") != "")

# 4. Analyze the Data
# Count the total number of reviews[cite: 4].
total_reviews = clean_df.count()
print(f"Total number of reviews after cleaning: {total_reviews}")

# Count how many reviews are labeled positive and negative[cite: 4].
print("--- Review Counts by Sentiment ---")
clean_df.groupBy("sentiment").count().show()

# 5. Transform the Data
# Create a new column that stores the length of each review (number of characters)[cite: 4].
transformed_df = clean_df.withColumn("review_length", length(col("review")))

# Filter and display reviews where the review length is greater than 500 characters[cite: 4].
print("--- Reviews Longer Than 500 Characters ---")
transformed_df.filter(col("review_length") > 500).show(5)

# 6. Save Your Results
# Save the cleaned and transformed DataFrame as a new CSV file[cite: 4].
transformed_df.write.csv("cleaned_movie_reviews", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()