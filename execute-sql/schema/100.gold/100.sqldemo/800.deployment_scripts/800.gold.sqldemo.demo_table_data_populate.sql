merge into {env}_gold.sqldemo.demo_table as target
using (
    select 'A' as string_key, 'demo val 1' as test_string
    union all select 'B' as string_key, 'another demo val' as test_string
    union all select 'C' as string_key, 'one more demo val' as test_string
) as source
on target.string_key = source.string_key

WHEN MATCHED AND (
    IFNULL(target.test_string, '') <> IFNULL(source.test_string, '') 

) THEN
    UPDATE SET
        target.test_string = source.test_string,
        is_active = TRUE,
        modified_date = CURRENT_TIMESTAMP()

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        string_key,
        test_string,
        modified_date,
        is_active
    )
    VALUES (
        source.string_key,
        source.test_string,
        CURRENT_TIMESTAMP(),
        TRUE
    )

WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET
        target.is_active = FALSE,
        target.modified_date = CURRENT_TIMESTAMP();


