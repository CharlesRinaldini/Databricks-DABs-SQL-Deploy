#import for using dbutils in python
# from databricks.sdk.runtime import *
from databricks.sdk import WorkspaceClient
import jaydebeapi
from datetime import datetime
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

useDatabricks = os.getenv('ENV') == "dv" 

if useDatabricks:
    from databricks.connect import DatabricksSession as SparkSession
    os.environ["USER"] = "cibuild"
else:
    from pyspark.sql import SparkSession

from pathlib import Path
#imports for sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
#datetime functions for folder and file naming 
from datetime import datetime
#import for creating output of notebook
import json
#import for interacting with Delta Lake
import delta
#import for creating unique temp table name
import uuid
#import for http requests
import requests
import base64
#for parallel running of notebooks
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count

def printWithTime(str):
  # Print a string with the current timestamp
  now = datetime.now()
  modifiedDatetime = now.strftime("%Y-%m-%d %H:%M:%S")
  printWithTime(f"{modifiedDatetime}: {str}")

class sql_helper:
  def __init__(self):
    #set variables use later
    envName = os.getenv('ENV')
    self.setSpark()

    try:
      path = Path(__file__)
      configPath = path.parent.parent.parent.parent.absolute()
      with open(os.path.join(configPath, "config", f"{envName}-config.json")) as f: 
        envConfig = (json.load(f))["envConfig"]
        
      scopeName = envConfig["scopeName"]
      # Set paths
      envName = envConfig["envName"]

      dbrURL = envConfig["databricksURL"]
      dbrTokenSecret = envConfig["databricksTokenSecret"]

      now = datetime.now()
      self.databricksURL = dbrURL
      self.databricksTokenSecret = dbrTokenSecret
      self.envName = envName

    except BaseException as e:
      if "Attribute `sparkContext` is not supported" in str(e):
        raise Exception("SparkContext is not available. Please run this notebook on a single user UC enabled cluster.")
      else: 
        raise Exception(str(e))
      
  def setSpark(self) -> SparkSession:
    self.spark = SparkSession.builder.getOrCreate()
    return self.spark
  
  def getSpark(self):
    return self.spark  
  
  def getDbutils(self):
    return self.workspaceClient.dbutils
  
  def getEnvName(self):
    # Get the environment's name
    return self.envName

  def invoke_sqlcmd_jdbc(self, dbHost, dbHTTPPath, dbToken, command):
    jdbc_url = (
        f"jdbc:spark://{dbHost}:443/default;"
        f"transportMode=http;"
        f"ssl=1;"
        f"httpPath={dbHTTPPath};"
        f"AuthMech=3;"
        f"UID=token;"
        f"PWD={dbToken}"
    )

    driver_class = "com.simba.spark.jdbc.Driver"

    # Update this path to wherever your JDBC JARs are stored
    jars = [
        "databricks-jdbc-3.4.2.jar"
    ]

    try:
        conn = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            ["token", dbToken],
            jars
        )
        cursor = conn.cursor()
        cursor.execute(command)
        cursor.close()
        conn.close()
        return "Pass"

    except Exception as ex:
        return str(ex)

  def invoke_sqlcmd_jdbc_fetch(self, dbHost, dbHTTPPath, dbToken, command):
    jdbc_url = (
        f"jdbc:spark://{dbHost}:443/default;"
        f"transportMode=http;"
        f"ssl=1;"
        f"httpPath={dbHTTPPath};"
        f"AuthMech=3;"
        f"UID=token;"
        f"PWD={dbToken}"
    )

    driver_class = "com.simba.spark.jdbc.Driver"

        # Update this path to wherever your JDBC JARs are stored
    jars = [
        "databricks-jdbc-3.4.2.jar"
    ]

    try:
        conn = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            ["token", dbToken],
            jars
        )
        cursor = conn.cursor()
        cursor.execute(command)

        row = cursor.fetchone()

        cursor.close()
        conn.close()

        return row

    except Exception as ex:
        printWithTime(f"JDBC fetch error: {ex}")
        return None

  def get_is_script_executed(self, file_name, dbHost, dbHTTPPath, dbToken):
    sql = f"""
        SELECT COALESCE((
            SELECT TOP 1 CASE WHEN ScriptExecutionLogId IS NOT NULL THEN 1 ELSE 0 END
            FROM audit.ScriptExecutionLog
            WHERE FileName = '{file_name}'
            AND ErrorMessage IS NULL
            ORDER BY ExecutionDateTime DESC
        ), 0) AS isExecuted
    """

    try:
        row = self.invoke_sqlcmd_jdbc_fetch(dbHost, dbHTTPPath, dbToken, sql)
        return row[0] == 1

    except Exception as ex:
        printWithTime(f"Error checking script execution: {ex}")
        return False

  def get_does_view_exist(self, view_name, dbHost, dbHTTPPath, dbToken):
    catalog, schema, table = view_name.split(".")

    sql = f"""
        SELECT 1 AS viewExists
        FROM system.information_schema.views
        WHERE table_catalog = lower('{catalog}')
        AND table_schema = lower('{schema}')
        AND table_name = lower('{table}')
    """

    try:
        row = self.invoke_sqlcmd_jdbc_fetch(dbHost, dbHTTPPath, dbToken, sql)
        return row is not None and row[0] == 1

    except Exception as ex:
        printWithTime(f"Error checking view existence: {ex}")
        return False

  def run_scripts(
      self,
      dbHost,
      dbPort,
      dbHTTPPath,
      dbToken,
      workingFolder,
      env,
      catalogFilter="",
      schemaFilter=""
  ):
    envLower = env.lower()
    envUpper = env.upper()

    working_path = Path(workingFolder)

    # Catalog filtering
    catalogs = (
        sorted([d for d in working_path.iterdir() if d.is_dir() and catalogFilter in d.name])
        if catalogFilter
        else sorted([d for d in working_path.iterdir() if d.is_dir()])
    )

    for catalog in catalogs:
        catalogName = catalog.name
        printWithTime(f"Processing Catalog: {catalogName}")

        # Schema filtering
        schemas = (
            sorted([d for d in catalog.iterdir() if d.is_dir() and schemaFilter in d.name])
            if schemaFilter
            else sorted([d for d in catalog.iterdir() if d.is_dir()])
        )

        for schema in schemas:
            schemaName = schema.name
            printWithTime(f"Processing Schema: {catalogName}.{schemaName}")

            objects = sorted([d for d in schema.iterdir() if d.is_dir()])

            for obj in objects:
                objectName = obj.name
                objectType = int(objectName.split(".")[0])

                checkScriptLog = objectType >= 900
                isView = 300 <= objectType < 400

                printWithTime(f"Processing Objects: {catalogName}.{schemaName}.{objectName}")

                scripts = sorted(obj.iterdir())

                for script in scripts:
                    scriptName = script.name
                    scriptText = script.read_text()

                    # Replace tokens
                    scriptCommand = (
                        scriptText.replace("{env}", envLower)
                        .replace("{envName}", envLower)
                        .replace("{envUpper}", envUpper)
                        .replace("{envNameUpper}", envUpper)
                    )

                    # View handling
                    if isView:
                        parts = scriptName.split(".")
                        viewObjectName = f"{envLower}_{parts[1]}.{parts[2]}.{parts[3]}"
                        printWithTime(f"Searching for:      {viewObjectName}")

                        doesExist = self.get_does_view_exist(viewObjectName, dbHost, dbHTTPPath, dbToken)

                        if doesExist:
                            printWithTime(f"Altering View:      {viewObjectName}")
                            scriptCommand = (
                                scriptCommand.replace("create view", "alter view")
                                            .replace("CREATE VIEW", "ALTER VIEW")
                            )
                        else:
                            printWithTime(f"Creating View:      {viewObjectName}")

                    # Script execution logging
                    if checkScriptLog:
                        printWithTime(f"Checking log for:   {scriptName}")
                        prevExecution = self.get_is_script_executed(
                            scriptName,
                            dbHost,
                            dbHTTPPath,
                            dbToken
                        )

                        if prevExecution:
                            printWithTime(f"Not running:        {catalogName}.{schemaName}.{objectName}.{scriptName}")
                            continue

                        printWithTime(f"Running Script:     {catalogName}.{schemaName}.{objectName}.{scriptName}")
                        execMessage = self.invoke_sqlcmd_jdbc(dbHost, dbHTTPPath, dbToken, scriptCommand)

                        if execMessage != "Pass":
                            errorMessage = execMessage[:2000]
                            logSql = (
                                "INSERT INTO audit.ScriptExecutionLog(FileName, ExecutionDateTime, ErrorMessage) "
                                f"SELECT '{scriptName}', GETUTCDATE(), '{errorMessage}'"
                            )
                            self.invoke_sqlcmd_jdbc_fetch(dbHost, dbHTTPPath, dbToken, logSql)
                            printWithTime(execMessage)
                        else:
                            logSql = (
                                "INSERT INTO audit.ScriptExecutionLog(FileName, ExecutionDateTime, ErrorMessage) "
                                f"SELECT '{scriptName}', GETUTCDATE(), NULL"
                            )
                            self.invoke_sqlcmd_jdbc_fetch(dbHost, dbHTTPPath, dbToken, logSql)

                    else:
                        printWithTime(f"Running Script:     {catalogName}.{schemaName}.{objectName}.{scriptName}")
                        execMessage = self.invoke_sqlcmd_jdbc(dbHost, dbHTTPPath, dbToken, scriptCommand)  

                        if execMessage != "Pass":
                            printWithTime("There was an error executing the following statement:")
                            printWithTime(execMessage)
