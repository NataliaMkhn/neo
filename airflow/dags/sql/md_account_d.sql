TRUNCATE TABLE dsl.md_account_d;

INSERT INTO dsl.md_account_d
SELECT to_date(fpf."DATA_ACTUAL_DATE", 'yyy-MM-dd')
     , to_date(fpf."DATA_ACTUAL_END_DATE", 'yyy-MM-dd')
	 , fpf."ACCOUNT_RK"
	 , fpf."ACCOUNT_NUMBER"
	 , fpf."CHAR_TYPE"
	 , fpf."CURRENCY_RK"
     , fpf."CURRENCY_CODE"::text
FROM stage.md_account_d fpf;

