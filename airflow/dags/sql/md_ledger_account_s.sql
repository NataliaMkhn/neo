TRUNCATE TABLE dsl.md_ledger_account_s;

INSERT INTO dsl.md_ledger_account_s
SELECT fpf."CHAPTER"
     , fpf."CHAPTER_NAME"
	 , fpf."SECTION_NUMBER"
	 , fpf."SECTION_NAME"
	 , fpf."SUBSECTION_NAME"
	 , fpf."LEDGER1_ACCOUNT"
	 , fpf."LEDGER1_ACCOUNT_NAME"
	 , fpf."LEDGER_ACCOUNT"
	 , fpf."LEDGER_ACCOUNT_NAME"
	 , fpf."CHARACTERISTIC"
	 , to_date(fpf."START_DATE", 'yyy-MM-dd')
	 , to_date(fpf."END_DATE", 'yyy-MM-dd')
FROM stage.md_ledger_account_s fpf;

