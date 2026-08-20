CREATE TABLE IF NOT EXISTS {env}_gold.sqldemo.script_execution_log (
    ScriptExecutionLogId BIGINT GENERATED ALWAYS AS IDENTITY,
    FileName STRING NOT NULL,
    ExecutionDateTime TIMESTAMP NOT NULL,
    ErrorMessage STRING,
    CONSTRAINT PK_ScriptExecutionLog PRIMARY KEY (ScriptExecutionLogId)
);
