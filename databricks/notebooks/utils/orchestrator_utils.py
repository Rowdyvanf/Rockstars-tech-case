# Databricks notebook source
# MAGIC %run ../config/Config

# COMMAND ----------

# MAGIC %run ./functions

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

class OrchestratorUtils(object):

    def __init__(self,process_name):
        self.process_name = process_name



    def initiate_songs_analysis(self):
        '''
        This function initiates the full process
        '''
        status,orchestrator = self.create_orchestrator()
        status = self.execute_orchestrator(orchestrator)


    def create_orchestrator(self):
        
        try:
            status = ''
            config_obj = Config()
            status = self.create_control_table(config_obj)
            status,orchestrator = self.insert_orch_steps(config_obj)
        except Exception as execution_error:
            print(f"create_orchestrator failed execution_error: {execution_error}")
            status = f"{execution_error}"
        return status,orchestrator

    def create_control_table(self,config_obj):
        try:
            #create the table without data if it doesnt exist yet
            db = config_obj.control_db
            table_name = config_obj.orchestrator_table_name
            orchestrator_table = f'{db}.{table_name}{self.process_name}'
            spark.sql(f'''
                    CREATE DATABASE IF NOT EXISTS {db}
                    ''')
            spark.sql(f'''
                    CREATE TABLE IF NOT EXISTS {orchestrator_table} (
                        id string,
                        step string,
                        parameters string,
                        status string,
                        log string
                    )
                    ''')
            if spark.catalog.tableExists(orchestrator_table):
                status = 'success'
            else:
                status = 'failed: orchestrator table whas not created'
        except Exception as execution_error:
            print(f"create_control_table failed execution_error: {execution_error}")
            status = f"{execution_error}"
        return status

    def insert_orch_steps(self,config_obj):
        try:
            # insert the steps from the configs file into the orchestrator table with the status 'to run'
            orch_steps = config_obj.orch_steps
            orchestrator_table = f'{config_obj.control_db}.{config_obj.orchestrator_table_name}{self.process_name}'
            orchestrator = spark.sql(f'''
                                    SELECT * FROM {orchestrator_table}
                                    ''')
            previous_steps = list(orchestrator.select('step').toPandas()['step'])
            for step in orch_steps.values():
                if step['name'] not in previous_steps:
                    schema = StructType([
                        StructField("id", StringType(), True),
                        StructField("step", StringType(), True),
                        StructField("parameters", StringType(), True),
                        StructField("status", StringType(), True),
                        StructField("log", StringType(), True)
                    ])
                    newRow = spark.createDataFrame([(step['id'],step['name'],'to run', '', step['parameters'])],schema=schema)
                    orchestrator = orchestrator.union(newRow)
                    orchestrator = orchestrator.orderBy('id')
            orchestrator.write.mode("overwrite").saveAsTable(orchestrator_table)
            status = 'success'
        except Exception as execution_error:
            print(f"insert_orch_steps failed execution_error: {execution_error}")
            status = f"{execution_error}"
        return status,orchestrator

    def execute_orchestrator(self, orchestrator):
        # execute all the steps in the orchestrator table that dont have a status as 'completed'
        try:
            dfp = orchestrator.toPandas()
            functions = Functions()
            for index, row in dfp.iterrows():
                if row['status'] != "completed":
                    print(row)
                    try:
                        function_name = row['step']
                        parameters = row['parameters']
                        function = getattr(functions, function_name)
                        function(parameters)
                        row['status'] = 'completed'
                        row['log'] = 'succes'
                    except Exception as execution_error:
                        print(f"executing {row['step']} failed execution_error: {execution_error}")
                        row['status'] = "failed"
                        row['log'] = f"execution_error: {execution_error}"
            orchestrator = spark.createDataFrame(dfp)
            orchestrator.write.mode("overwrite").saveAsTable(orchestrator_table)
            status = 'success'
        except Exception as execution_error:
            print(f"execute_orchestrator failed execution_error: {execution_error}")
            status = f"{execution_error}"
        return status
