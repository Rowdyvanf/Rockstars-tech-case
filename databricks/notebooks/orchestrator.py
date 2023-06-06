# Databricks notebook source
# MAGIC %run ./utils/orchestrator_utils

# COMMAND ----------

process_name = '3'
orch_obj = OrchestratorUtils(process_name)
orch_obj.initiate_songs_analysis()
