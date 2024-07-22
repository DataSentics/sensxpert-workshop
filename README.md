# sensXPERT workshop
24.7. and 25.7.2024

## Setup

For most of the code to work run first cell in `1_intro_DBX.py` it sets
up data in hivematestore.

## Day one (24. 7.):
  Databricks engineering: TBA

## Day two
  - AI assistant in `2_Data_science/0_AI_assistants.py`
  - MLFlow in `2_Data_science/1_MLflow.py`
  - AutoML (optional) in `2_Data_science/1_AutoML.ipynb`
  - 

## Materials:
- DBX_intro
    - 1_intro_DBX - (notebooks, clusters, workflows)
- DBX data engineering
    - 1_data_lakehouse - medallion architecture philosophy
    - 2_Delta - Delta format in detail (partitioning, time travel etc.)
    - 2_pyspark - pyspark transformations, pyspark vs. pandas
- DBX data science
    - 0_AI_assistant - examples where it is useful
    - 1_AutoML - automl example bridge to MLflow
    - 2_MLflow - MLflow experiment tracking
    - 3_Effective_model_training - (hyperopt, applyInPandas, spark inference)
    - 4_Feature_store - FS philosophy
    - 6_LLM Example - example of loading pretraind model
    - 7_LLM_RAG - example of simple RAG
- Git
    - 0_what_is_git - in generall what is git
    - 1_setting_up_git - git in DBX
    - 2_git_practice - how to use git
- Python
    - 0_Introduction - notebooks capabilities
    - 1_Pandas - Pandas vs. SQL (good for people who are familiar with SQL and needs to learn Pandas)
    - 2_Scikit-learn - simple example of model training
    - 3_Decorators - examples how to use decorators in python
- SQL
    - TODO query example
    - TODO alert example
    - TODO dashboard example
- Feature store
    - 0_feature-store-basic-example - how to use FeatureStoreClient
    - 1_odap_feature_store - described difference to ODAP featurestore
  
- Advanced topics
    - data_quality_monitoring:
        - 0_basic_monitoring - monitoring with custom functions
        - 1_lakehouse_monitoring_demo - set-up of lakehouse monitoring
        - 2_QI_features_monitoring - example of using the quick insights library
    - streaming_workshop
        - generate_artigicial_stream - this notebook generate the stream. It must run during the workshop.
        - streaming_workshop - examples of operations on streams

