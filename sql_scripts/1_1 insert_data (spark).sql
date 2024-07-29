create or replace procedure ds.fill_all_table()

language 'plpgsql'
AS $$
declare

    log_txt text;
    err_code text;
    msg_text text;
    t_name text;
    time_sleep integer;

begin

    begin   --блок для отслеживания ошибки

        t_name = 'ds.md_account_d';
        log_txt =  concat('загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);


        merge into ds.md_account_d a
        using ds.t_account_d t
            on a.data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd') and 
		       a.account_rk = to_number(t."ACCOUNT_RK", '9999999999')
        when matched then
            update set data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                       data_actual_end_date = to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                       account_rk = to_number(t."ACCOUNT_RK", '9999999999'),
                       account_number = t."ACCOUNT_NUMBER",
                       char_type = t."CHAR_TYPE",
                       currency_rk = to_number(t."CURRENCY_RK", '9999999999'),
                       currency_code = t."CURRENCY_CODE"
        when not matched then
            insert 
                values (to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                        to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                        to_number(t."ACCOUNT_RK", '9999999999'),
                        t."ACCOUNT_NUMBER",
                        t."CHAR_TYPE",
                        to_number(t."CURRENCY_RK", '9999999999'),
                        t."CURRENCY_CODE"
			            );

        drop table ds.t_account_d;

        t_name = 'ds.md_currency_d';
        log_txt =  concat('загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        merge into ds.md_currency_d c
        using ds.t_currency_d t
            on c.data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd') and 
	           c.currency_rk = to_number(t."CURRENCY_RK", '9999999999')
        when matched then
            update set currency_rk = to_number(t."CURRENCY_RK" , '9999999999'),
                       data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                       data_actual_end_date = to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                       currency_code = t."CURRENCY_CODE",
                       code_iso_char = t."CODE_ISO_CHAR"
        when not matched then
            insert 
                values (to_number(t."CURRENCY_RK" , '9999999999'),
                        to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                        to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                        t."CURRENCY_CODE",
                        t."CODE_ISO_CHAR");

        drop table ds.t_currency_d;        

        t_name = 'ds.md_exchange_rate_d';
        log_txt =  concat('загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        merge into ds.md_exchange_rate_d m
        using ds.t_exchange_rate_d t
            on m.data_actual_date =  to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd') and 
		       m.currency_rk = to_number(t."CURRENCY_RK", '9999999999')
        when matched then
            update set data_actual_date = to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                       data_actual_end_date = to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                       currency_rk = to_number(t."CURRENCY_RK", '9999999999'),
                       reduced_cource = cast(t."REDUCED_COURCE" as numeric), 
                       code_iso_num = t."CODE_ISO_NUM"
        when not matched then
            insert 
                values (to_date(t."DATA_ACTUAL_DATE", 'yyy-MM-dd'),
                        to_date(t."DATA_ACTUAL_END_DATE", 'yyy-MM-dd'),
                        to_number(t."CURRENCY_RK", '9999999999'),
                        cast(t."REDUCED_COURCE" as numeric), 
                        t."CODE_ISO_NUM");

        drop table ds.t_exchange_rate_d;

        t_name = 'ds.md_ledger_account_s';
        log_txt =  concat('загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        merge into ds.md_ledger_account_s l
        using ds.t_ledger_account_s t
            on l.ledger_account = to_number(t."LEDGER_ACCOUNT", '99999') and 
			   l.start_date = to_date(t."START_DATE", 'yyy-MM-dd')
        when matched then
            update set
                chapter = t."CHAPTER",
                chapter_name = t."CHAPTER_NAME",
                section_number = to_number(t."SECTION_NUMBER", '999'),
                section_name = t."SECTION_NAME", 
                subsection_name = t."SUBSECTION_NAME",
                ledger1_account = to_number(t."LEDGER1_ACCOUNT", '999'),
                ledger1_account_name = t."LEDGER1_ACCOUNT_NAME",
                ledger_account = to_number(t."LEDGER_ACCOUNT", '99999'),
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
                        to_number(t."SECTION_NUMBER", '999'),
                        t."SECTION_NAME", 
                        t."SUBSECTION_NAME",
                        to_number(t."LEDGER1_ACCOUNT", '999'),
                        t."LEDGER1_ACCOUNT_NAME",
                        to_number(t."LEDGER_ACCOUNT", '99999'),
                        t."LEDGER_ACCOUNT_NAME",
                        t."CHARACTERISTIC", 
                        to_date(t."START_DATE", 'yyy-MM-dd'), 
                        to_date(t."END_DATE", 'yyy-MM-dd'));

        drop table ds.t_ledger_account_s;

        t_name = 'ds.ft_balance_f';
        log_txt =  concat('загрузка данных в таблицу ', t_name);
        call logs.log_write('info', log_txt);

        merge into ds.ft_balance_f b
        using ds.t_balance_f t
            on b.on_date = to_date(t."ON_DATE", 'dd.MM.yyy') and 
               b.account_rk = to_number(t."ACCOUNT_RK", '9999999999')
        when matched then
            update set
                       on_date = to_date(t."ON_DATE", 'dd.MM.yyy'),
                       account_rk = to_number(t."ACCOUNT_RK", '9999999999'),
                       currency_rk = to_number(t."CURRENCY_RK", '9999999999'),
                       balance_out = cast(t."BALANCE_OUT" as numeric)
        when not matched then
            insert 
                values (to_date(t."ON_DATE", 'dd.MM.yyy'),
                        to_number(t."ACCOUNT_RK", '9999999999'),
                        to_number(t."CURRENCY_RK", '9999999999'),
                        cast(t."BALANCE_OUT" as numeric));

        drop table ds.t_balance_f;

        t_name = 'ds.ft_posting_f p';
        log_txt =  concat('загрузка данных в таблицу ', t_name);
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
			                       to_number("CREDIT_ACCOUNT_RK", '9999999999'),
			                       to_number("DEBET_ACCOUNT_RK", '9999999999'),
			                       cast("CREDIT_AMOUNT" as numeric),
			                       cast("DEBET_AMOUNT" as numeric)
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

end; $$ 
