CREATE TABLE IF NOT EXISTS {env}_gold.sqldemo.demo_table (
    demo_id BIGINT GENERATED ALWAYS AS IDENTITY,
    string_key STRING NOT NULL,
    test_string STRING NOT NULL, 
    modified_date TIMESTAMP NOT NULL,
    is_active BOOLEAN 
);
