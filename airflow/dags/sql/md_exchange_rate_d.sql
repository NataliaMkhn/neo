TRUNCATE TABLE dsl.md_exchange_rate_d;

INSERT INTO dsl.md_exchange_rate_d
SELECT DISTINCT to_date(fpf."DATA_ACTUAL_DATE", 'yyy-MM-dd')
     , to_date(fpf."DATA_ACTUAL_END_DATE", 'yyy-MM-dd')
	 , fpf."CURRENCY_RK"
	 , fpf."REDUCED_COURCE"::numeric
	 , fpf."CODE_ISO_NUM"::text
FROM stage.md_exchange_rate_d fpf;

