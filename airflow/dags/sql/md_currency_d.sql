TRUNCATE TABLE dsl.md_account_d;

INSERT INTO dsl.md_currency_d
SELECT fpf."CURRENCY_RK"
     , to_date(fpf."DATA_ACTUAL_DATE", 'yyy-MM-dd')
     , to_date(fpf."DATA_ACTUAL_END_DATE", 'yyy-MM-dd')
     , fpf."CURRENCY_CODE"::text
     , fpf."CODE_ISO_CHAR"
FROM stage.md_currency_d fpf;

