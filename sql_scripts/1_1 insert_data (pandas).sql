-- PROCEDURE: ds.fill_all_table()

-- DROP PROCEDURE IF EXISTS ds.fill_all_table();

CREATE OR REPLACE PROCEDURE ds.fill_all_table(
	)
LANGUAGE 'plpgsql'
AS $BODY$
declare

    log_txt text;
    err_code text;
    msg_text text;
    t_name text;
    time_sleep integer;

begin

    begin   --блок для отслеживания ошибки

        log_txt =  'ЗАГРУЗКА ДАННЫХ';
        call logs.log_write('info', log_txt);

t_name = 'ds.md_account_d';
        log_txt =  concat('   загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        merge into ds.md_account_d a
        using ds.t_account_d t
            on a.data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd') and 
		       a.account_rk = t."ACCOUNT_RK"
        when matched then
            update set data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                       data_actual_end_date = to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                       account_rk = t."ACCOUNT_RK",
                       account_number = t."ACCOUNT_NUMBER",
                       char_type = t."CHAR_TYPE",
                       currency_rk = t."CURRENCY_RK",
                       currency_code = t."CURRENCY_CODE"::text
        when not matched then
            insert 
                values (to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                        to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                        t."ACCOUNT_RK", 
                        t."ACCOUNT_NUMBER",
                        t."CHAR_TYPE",
                        t."CURRENCY_RK", 
                        t."CURRENCY_CODE"::text
			            );

        drop table ds.t_account_d;

        t_name = 'ds.md_currency_d';
        log_txt =  concat('   загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        merge into ds.md_currency_d c
        using ds.t_currency_d t
            on c.data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd') and 
	           c.currency_rk = t."CURRENCY_RK"
        when matched then
            update set currency_rk = t."CURRENCY_RK",
                       data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                       data_actual_end_date = to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                       currency_code = t."CURRENCY_CODE"::text,
                       code_iso_char = t."CODE_ISO_CHAR"
        when not matched then
            insert 
                values (t."CURRENCY_RK",
                        to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                        to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                        t."CURRENCY_CODE"::text,
                        t."CODE_ISO_CHAR");

        drop table ds.t_currency_d;        

        t_name = 'ds.md_exchange_rate_d';
        log_txt =  concat('   загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);


        merge into ds.md_exchange_rate_d m
        using (select distinct * from ds.t_exchange_rate_d) t
            on m.data_actual_date =  to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd') and 
		       m.currency_rk = t."CURRENCY_RK"
        when matched then
            update set data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                       data_actual_end_date = to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                       currency_rk = t."CURRENCY_RK",
                       reduced_cource = t."REDUCED_COURCE"::numeric, 
                       code_iso_num = t."CODE_ISO_NUM"::text
        when not matched then
            insert 
                values (to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                        to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                        t."CURRENCY_RK",
                        t."REDUCED_COURCE"::numeric, 
                        t."CODE_ISO_NUM"::text);

        drop table ds.t_exchange_rate_d;

        t_name = 'ds.md_ledger_account_s';
        log_txt =  concat('   загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        merge into ds.md_ledger_account_s l
        using ds.t_ledger_account_s t
            on l.ledger_account = t."LEDGER_ACCOUNT" and 
			   l.start_date = to_date(t."START_DATE", 'yyy-MM-dd')
        when matched then
            update set
                chapter = t."CHAPTER",
                chapter_name = t."CHAPTER_NAME",
                section_number = t."SECTION_NUMBER",
                section_name = t."SECTION_NAME", 
                subsection_name = t."SUBSECTION_NAME",
                ledger1_account = t."LEDGER1_ACCOUNT",
                ledger1_account_name = t."LEDGER1_ACCOUNT_NAME",
                ledger_account = t."LEDGER_ACCOUNT",
                ledger_account_name = t."LEDGER_ACCOUNT_NAME",
                characteristic = t."CHARACTERISTIC", 
                start_date = to_date(t."START_DATE", 'yyy-MM-dd'), 
                end_date = to_date(t."END_DATE", 'yyy-MM-dd')   
        when not matched then
            insert (chapter, chapter_name, section_number, 
                    section_name, subsection_name, ledger1_account, ledger1_account_name, 
                    ledger_account, ledger_account_name, characteristic, start_date, end_date )
                values ("CHAPTER",
                        t."CHAPTER_NAME",
                        t."SECTION_NUMBER",
                        t."SECTION_NAME", 
                        t."SUBSECTION_NAME",
                        t."LEDGER1_ACCOUNT",
                        t."LEDGER1_ACCOUNT_NAME",
                        t."LEDGER_ACCOUNT",
                        t."LEDGER_ACCOUNT_NAME",
                        t."CHARACTERISTIC", 
                        to_date(t."START_DATE", 'yyy-MM-dd'), 
                        to_date(t."END_DATE", 'yyy-MM-dd'));

        drop table ds.t_ledger_account_s;

        t_name = 'ds.ft_balance_f';
        log_txt =  concat('   загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        merge into ds.ft_balance_f b
        using ds.t_balance_f t
            on b.on_date = to_date(t."ON_DATE", 'dd.MM.yyy') and 
               b.account_rk = t."ACCOUNT_RK"
        when matched then
            update set
                       on_date = to_date(t."ON_DATE", 'dd.MM.yyy'),
                       account_rk = t."ACCOUNT_RK",
                       currency_rk = t."CURRENCY_RK",
                       balance_out = t."BALANCE_OUT"::numeric
        when not matched then
            insert 
                values (to_date(t."ON_DATE", 'dd.MM.yyy'),
                        t."ACCOUNT_RK",
                        t."CURRENCY_RK", 
                        t."BALANCE_OUT"::numeric);

        drop table ds.t_balance_f;

        t_name = 'ds.ft_posting_f p';
        log_txt =  concat('   загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        truncate table ds.ft_posting_f;

        insert into ds.ft_posting_f (
			                         oper_date,
			                         credit_account_rk,
			                         debet_account_rk,
			                         credit_amount,
			                         debet_amount 
		                             )
			                select 
			                       to_date("OPER_DATE", 'dd-MM-yyy'),
			                       "CREDIT_ACCOUNT_RK", 
			                       "DEBET_ACCOUNT_RK",
			                       "CREDIT_AMOUNT"::numeric,
			                       "DEBET_AMOUNT"::numeric
			                from ds.t_posting_f;

  
        drop table ds.t_posting_f;

        time_sleep = 5;
        perform pg_sleep(5);

        log_txt =  'загрузка данных в таблицы завершена ';
        call logs.log_write('info', log_txt);

    exception 
        when others
            then 
                get stacked diagnostics
                    err_code = returned_sqlstate,
                    msg_text = message_text;

                log_txt =  concat('   заполнение таблиц завершено ошибкой:  ', err_code, ' ', msg_text);

                call logs.log_write('error', log_txt);  
    end;

end; 
$BODY$;
ALTER PROCEDURE ds.fill_all_table()
    OWNER TO dwh_user;
