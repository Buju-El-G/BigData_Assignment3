# -*- coding: utf-8 -*-
"""Generate Clipped ALS Recommendations from Saved Model

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1QzesvqYiJBJMKt2ZQzU5gwnuOcj8ATwc
"""

# generate_recommendations_spark.py
import time
import logging
import traceback
from pathlib import Path

# Import Spark specific modules
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALSModel # Load the saved model
from pyspark.sql.types import IntegerType, ArrayType, StructType, StructField, FloatType

# --- Configuration ---
# Directory where results from the previous run are stored
PREVIOUS_OUTPUT_DIR = Path("./spark_recommender_results_hashing")
# Path where the ALS model was saved
MODEL_PATH = str(PREVIOUS_OUTPUT_DIR / "als_model_hashing")
# Output directory for this script's log (optional)
OUTPUT_DIR = Path("./spark_recommendation_generation")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Log file configuration
LOG_FILE = OUTPUT_DIR / "generate_recommendations.log"

# Spark Configuration (Lower memory should be sufficient for just loading model + inference)
SPARK_DRIVER_MEMORY = "4g"
SPARK_EXECUTOR_MEMORY = "4g" # Keep same as driver in local mode
SPARK_KRYO_BUFFER_MAX = "512m" # Keep just in case

# Recommendation Configuration
# Use the User IDs (hashed integers) from your previous successful run's output
USER_IDS_TO_RECOMMEND = [1698346, 3511597, 4902608]
NUM_RECOMMENDATIONS = 5
# --- End Configuration ---

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'), # Overwrite log each time
        logging.StreamHandler()
    ]
)

logging.info("="*50)
logging.info("Recommendation Generation Script Started.")
script_start_time = time.time()

# --- Initialize Spark Session ---
logging.info("Initializing Spark Session...")
spark = None
try:
    spark = SparkSession.builder \
        .appName("ALSRecommendFromSaved") \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
        .config("spark.kryoserializer.buffer.max", SPARK_KRYO_BUFFER_MAX) \
        .master("local[*]") \
        .getOrCreate()
    logging.info(f"Spark Session created. Spark version: {spark.version}")
    logging.info(f"Driver Memory: {SPARK_DRIVER_MEMORY}, Executor Memory: {SPARK_EXECUTOR_MEMORY}")
except Exception as e:
    logging.error(f"Fatal Error creating Spark Session: {e}")
    logging.error(traceback.format_exc())
    exit()

# --- Load Saved Model and Generate Recommendations ---
loaded_model = None
try:
    # Load the previously saved ALS model
    logging.info(f"Loading saved ALS model from: {MODEL_PATH}")
    loaded_model = ALSModel.load(MODEL_PATH)
    logging.info("ALS model loaded successfully.")

    # Create a DataFrame with the user IDs to get recommendations for
    # Ensure the column name matches what the model expects ('userIdInt')
    user_schema = StructType([StructField("userIdInt", IntegerType(), False)])
    users_df = spark.createDataFrame([(id,) for id in USER_IDS_TO_RECOMMEND], schema=user_schema)
    logging.info(f"Created DataFrame for users: {USER_IDS_TO_RECOMMEND}")
    users_df.show()

    # Generate recommendations using the loaded model
    logging.info(f"Generating top {NUM_RECOMMENDATIONS} recommendations...")
    userRecs = loaded_model.recommendForUserSubset(users_df, NUM_RECOMMENDATIONS)
    logging.info("Generated raw recommendations.")
    userRecs.show(truncate=False)

    # Clip recommendation ratings to [1.0, 5.0] range
    logging.info("Clipping recommendation ratings...")
    userRecs_clipped = userRecs.withColumn(
        "recommendations_clipped",
        F.transform(
            F.col("recommendations"),
            lambda x: F.struct(
                x["itemIdInt"].alias("itemIdInt"),
                F.when(x["rating"] > 5.0, 5.0)
                 .when(x["rating"] < 1.0, 1.0)
                 .otherwise(x["rating"]).alias("rating") # Apply clipping
            )
        )
    )
    logging.info("Recommendation ratings clipped.")
    userRecs_clipped.select("userIdInt", "recommendations_clipped").show(truncate=False)


    # Collect and print the final clipped recommendations
    print("\n--- Final Clipped Recommendations ---")
    print(f"(Note: User/Item IDs are hashed integers. Ratings are clipped to [1,5])")
    userRecs_list = userRecs_clipped.collect()
    for row in userRecs_list:
        user_id = row['userIdInt']
        recs = row['recommendations_clipped']
        rec_string = ", ".join([f"(Item: {r['itemIdInt']}, Rating: {r['rating']:.4f})" for r in recs])
        print(f"User {user_id}: {rec_string}")


except Exception as e:
    logging.error(f"An error occurred during model loading or recommendation: {e}")
    logging.error(traceback.format_exc())

# --- Cleanup ---
finally:
    logging.info("Stopping Spark Session...")
    if spark: spark.stop()
    logging.info("Spark Session stopped.")

script_end_time = time.time()
total_time = script_end_time - script_start_time
logging.info("--- Recommendation Generation Completed ---")
logging.info(f"Total script time taken: {total_time:.2f} seconds")
print(f"\n--- Recommendation Generation Completed ---")
print(f"Total time taken: {total_time:.2f} seconds")