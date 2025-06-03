# Big Data Analytics - Assignment 3

This repository contains the code and instructions for Assignment 3 of the Big Data Analytics course (COMP 3610 @ U.W.I). The project involves analyzing the McAuley-Lab/Amazon-Reviews-2023 dataset using Dask and Apache Spark.


## Repository Structure

├── README.md                 # This file
├── environment.yml           # Conda environment specification
├── scripts/                  # Python scripts for each task│   
├── download_data_server.py    # Task 1: Data Acquisition│   
├── process_data_server2.py       # Task 2: Data Cleaning & Preprocessing│   
├── eda_server.py             # Task 3: Exploratory Data Analysis (Dask)│   
├── sentiment_analysis_spark.py # Task 4: Sentiment Analysis (Spark)│   
├── als_recommender_spark_hashing.py # Task 5: ALS Training (Spark, Hashing)│   
├── als_recommendation_generation_spark.py     # Task 5: Generate Recs from saved model│   
├── clustering_spark_no_brand.py             # Task 6: K-Means Clustering (Spark)│   
└── [bigdata_a3_utils.py]     # Utility script│
├── results/spark_sentiment_results/    # Output from Task 4
├── results/spark_recommender_results_hashing/ # Output from Task 5
├── results/spark_clustering_results_no_brand/ # Output from Task 6
├── results/spark_recommendation_generation/  # Logs from recommendation generation
└── results/eda_plots/                    # Output plots from Task 3
## Setup

### Prerequisites

* Conda (Miniconda or Anaconda)
* Access to a Linux environment (tested on Ubuntu 20.04) with sufficient resources (e.g., >= 32GB RAM, ample disk space for data).

### Environment Setup

1.  Clone or download this repository.
2.  Navigate to the repository's root directory (`BigData_A3`).
3.  Create the Conda environment using the provided file:
    ```bash
    conda env create -f environment.yml
    ```
4.  Activate the environment:
    ```bash
    conda activate bigdata_env
    ```

### Data Setup

* The scripts assume the cleaned Parquet data from Task 2 exists in the directory `/home/bigdata/amazon_data_cleaned/`.
* If this path is different, update the `CLEANED_PATH` variable in the relevant scripts (`eda_server.py`, `sentiment_analysis_spark.py`, `als_recommender_system_spark.py`, `clustering_spark.py`).
* The raw data download (Task 1) and cleaning (Task 2) scripts should be run first if the cleaned data is not already present.

## Running the Scripts

All scripts should be run from the repository's root directory (`BigData_A3`) after activating the `bigdata_env` Conda environment.

**Important Note on Long-Running Jobs:** Tasks 4 (Sentiment Analysis) and 5 (ALS Training) can take many hours (potentially 10-25+ hours) depending on the hardware. It is **highly recommended** to run these scripts inside a terminal multiplexer like `tmux` to prevent termination due to SSH disconnections.

```bash
# Example using tmux:
tmux              # Start a new tmux session
conda activate bigdata_env
cd ~/BigData_A3   # Navigate to project root
python scripts/sentiment_analysis_spark.py # Start the long script
# Press Ctrl+b, then d to detach from tmux (script keeps running)
# To re-attach later: tmux attach
Task 1: Data Acquisition python scripts/download_data_server.py
Task 2: Data Cleaning & Preprocessing python scripts/process_data_server2.py
Task 3: Exploratory Data Analysis (EDA) python scripts/eda_server.py
    Outputs plots to eda_plots/.Prints correlation results to console.
Task 4: Sentiment Analysis (Spark)# Use tmux (see above) python scripts/sentiment_analysis_spark.py
    Outputs results to spark_sentiment_results/.Logs append to spark_sentiment_results/sentiment_analysis_spark.log.
Task 5: Recommender System - ALS Training (Spark)# Use tmux (see above) python scripts/als_recommender_spark_hashing.py
    Outputs results (model, log, results text file) to spark_recommender_results_hashing/.
Task 5: Generate Clipped Recommendations (Run after ALS training completes)python scripts/generate_recommendations.py
    Loads the saved ALS model.Prints clipped recommendations to the console.Logs to spark_recommendation_generation/.
Task 6: K-Means Clustering (Spark)# Use tmux if runtime is expected to be long, though likely faster (~1 hour) python scripts/clustering_spark.py
    Outputs results (models, log, results text file) to spark_clustering_results_no_brand/.NotesSpark scripts are configured to run in local[*] mode, utilizing all available cores on the     machine.Memory configurations (spark.driver.memory, spark.executor.memory) are set within the scripts but may need adjustment based on the specific execution environment.

Notes:

Spark scripts are configured to run in local[*] mode, utilizing all available cores on the machine.

Memory configurations (spark.driver.memory, spark.executor.memory) are set within the scripts but may need adjustment based on the specific execution environment.
