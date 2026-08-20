import jaydebeapi
from datetime import datetime
import os
import sys
from pathlib import Path
import json
from datetime import datetime

def printWithTime(str):
  # Print a string with the current timestamp
  now = datetime.now()
  modifiedDatetime = now.strftime("%Y-%m-%d %H:%M:%S")
  print(f"{modifiedDatetime}: {str}")

class deploy_sql:
  def __init__(self):
    #set variables use later
    envName = os.getenv('ENV')

    try:
      path = Path(__file__)
      configPath = path.parent.parent.parent.parent.absolute()
      with open(os.path.join(configPath, "config", f"{envName}-config.json")) as f: 
        envConfig = (json.load(f))["envConfig"]
        
      envName = envConfig["envName"]

      dbrURL = envConfig["databricksURL"]
      dbrToken = envConfig["databricksToken"]
      dbrHTTPPath = envConfig["databricksHTTPPath"]
      dbrHost = envConfig["databricksHost"]

      now = datetime.now()
      self.envName = envName
      self.databricksURL = dbrURL
      self.databricksToken = dbrToken
      self.databricksHTTPPath = dbrHTTPPath
      self.databricksHost = dbrHost

    except BaseException as e:
      if "Attribute `sparkContext` is not supported" in str(e):
        raise Exception("SparkContext is not available. Please run this notebook on a single user UC enabled cluster.")
      else: 
        raise Exception(str(e))
      
  def getEnvName(self):
    # Get the environment's name
    return self.envName

  def getDatabricksHTTPPath(self):
    # Get the Databricks HTTP path
    return self.databricksHTTPPath

  def getDatabricksHost(self):
    # Get the Databricks host
    return self.databricksHost

  def getDatabricksURL(self):
    # Get the Databricks URL
    return self.databricksURL

  def getDatabricksToken(self):
    # Get the Databricks token
    return self.databricksToken

  def invoke_sqlcmd_jdbc(self, dbHost, dbHTTPPath, dbToken, command):
    jdbc_url = f"jdbc:databricks://{dbHost}:443;httpPath={dbHTTPPath};AuthMech=3;UID=token;PWD={dbToken}"

    driver_class = "com.databricks.client.jdbc.Driver"
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent.parent.parent
    jar_path = str(project_root / "drivers" / "databricks-jdbc-3.4.2.jar")

    try:
        conn = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            ["token", dbToken],
            jar_path
        )
        cursor = conn.cursor()
        cursor.execute(command)
        cursor.close()
        conn.close()
        return "Pass"

    except Exception as ex:
        return str(ex)

  def invoke_sqlcmd_jdbc_fetch(self, dbHost, dbHTTPPath, dbToken, command):
    jdbc_url = f"jdbc:databricks://{dbHost}:443;httpPath={dbHTTPPath};AuthMech=3;UID=token;PWD={dbToken}"

    driver_class = "com.databricks.client.jdbc.Driver"
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent.parent.parent
    jar_path = str(project_root / "drivers" / "databricks-jdbc-3.4.2.jar")

    try:
        conn = jaydebeapi.connect(
            driver_class,
            jdbc_url,
            ["token", dbToken],
            jar_path
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
    env = self.getEnvName()
    sql = f"""
        SELECT COALESCE((
            SELECT CASE WHEN ScriptExecutionLogId IS NOT NULL THEN 1 ELSE 0 END
            FROM {env}_gold.sqldemo.script_execution_log
            WHERE FileName = '{file_name}'
            AND ErrorMessage IS NULL
            ORDER BY ExecutionDateTime DESC
            LIMIT 1
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
                            errorMessage = execMessage[:200]
                            logSql = (
                                f"INSERT INTO {env}_gold.sqldemo.script_execution_log(FileName, ExecutionDateTime, ErrorMessage) "
                                f"SELECT '{scriptName}', current_timestamp(), '{errorMessage}'"
                            )
                            self.invoke_sqlcmd_jdbc_fetch(dbHost, dbHTTPPath, dbToken, logSql)
                            printWithTime(execMessage[:200])
                        else:
                            logSql = (
                                f"INSERT INTO {env}_gold.sqldemo.script_execution_log(FileName, ExecutionDateTime, ErrorMessage) "
                                f"SELECT '{scriptName}', current_timestamp(), NULL"
                            )
                            self.invoke_sqlcmd_jdbc_fetch(dbHost, dbHTTPPath, dbToken, logSql)

                    else:
                        printWithTime(f"Running Script:     {catalogName}.{schemaName}.{objectName}.{scriptName}")
                        execMessage = self.invoke_sqlcmd_jdbc(dbHost, dbHTTPPath, dbToken, scriptCommand)  

                        if execMessage != "Pass":
                            printWithTime("There was an error executing the following statement:")
                            printWithTime(execMessage[:200])
def main(workingFolder):
    # Get the environment from the ENV environment variable
    env = os.getenv('ENV')
    # Create an instance of deploy_sql
    deployer = deploy_sql()
    
    # Get Databricks connection details
    dbHost = deployer.getDatabricksHost()
    dbHTTPPath = deployer.getDatabricksHTTPPath()
    dbToken = deployer.getDatabricksToken()
    
    # Run the SQL scripts
    deployer.run_scripts(dbHost, dbHTTPPath, dbToken, workingFolder, env)

if __name__ == "__main__":
    sys.exit(main("C:\\Users\\176017\\sources\\github\\Databricks\\execute-sql\\schema"))