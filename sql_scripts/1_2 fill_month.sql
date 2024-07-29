-- задача 1_2 расчет витрин dm.dm_account_balance_f и dm.dm_account_turnover_f
-- входной параметр такойже, как для расчета формы 101. если передается 01-02-2018, то витрины заполняются за январь 2018
create or replace procedure ds.fill_month_dm(
                                             i_OnDate date
                                             )
language 'plpgsql'
AS $$
declare

	v_start_date date;
	v_end_date date;
	v_date record;
    v_in_date date;

    log_txt text;
    err_code text;
    msg_text text;

begin
    log_txt =  ('РАСЧЕТ ВИТРИН');
    call logs.log_write('info', log_txt);

    begin   --блок для отслеживания ошибки

        -- определяем диапазон дат для января 2018
        v_start_date = i_OnDate - interval '1 month';   -- дата начала месяца для расчета
        v_end_date = i_OnDate - interval '1 day';       -- дата конца месяца для расчета
        v_in_date = v_start_date - interval '1 day';    -- дата для входящих остатков

        -- расчет для всех дат января, по которым есть обороты
        for v_date in (
                       select distinct oper_date 
                       from ds.ft_posting_f 
                       where oper_date between v_start_date and v_end_date
                       order by oper_date
                   )
		    loop
                call ds.fill_account_turnove_f(v_date.oper_date::date);
            end loop;


        -- заполнение остатков на 31.12.2017 для счетов, действующих на 01.01.2018

        log_txt =  concat('заполнение остатков в витрину dm.dm_account_balance_f за ', v_in_date);
        call logs.log_write('info', log_txt);

        delete from dm.dm_account_balance_f
        where dm.dm_account_balance_f.on_date = v_in_date;


        insert into dm.dm_account_balance_f
                                 (on_date,
                                  account_rk,
                                  balance_out,
                                  balance_out_rub
                                  )
       with c_query as (
                         select currency_rk, reduced_cource
                         from ds.md_exchange_rate_d
                         where v_in_date between ds.md_exchange_rate_d.data_actual_date and 
                                               ds.md_exchange_rate_d.data_actual_end_date
                         )
        select on_date,
               ds.md_account_d.account_rk,
               balance_out,
               balance_out * coalesce(c_query.reduced_cource, 1) as balance_out_rub
        from ds.ft_balance_f
        inner join ds.md_account_d
              using (account_rk)
        left  join c_query
              on ds.ft_balance_f.currency_rk = c_query.currency_rk
        where v_start_date between ds.md_account_d.data_actual_date and 
                                   ds.md_account_d.data_actual_end_date
                           and ds.ft_balance_f.on_date = v_in_date;
 
        log_txt =  concat('остатки за ', v_in_date, ' заполнены');
        call logs.log_write('info', log_txt);

        for v_date in (
                 select * from generate_series(
                                  to_date('2018-01-01', 'yyy-MM-dd'), 
                                  (to_date('2018-01-01','YYYY-MM-DD') + interval '1 month' - interval '1 day'), '1 day'
                                               ) as date_list
		               )
	    	loop
                call ds.fill_account_balance_f(v_date.date_list::date);
            end loop;

    exception 
        when others
             then 

                 get stacked diagnostics
                     err_code = returned_sqlstate,
                     msg_text = message_text;

                 log_txt =  concat('   расчет витрины dm.dm_account_balance_f за ', i_OnDate::text, 
                                   ' завершен ошибкой:  ', err_code, ' ', msg_text);

                 call logs.log_write('error', log_txt);  
    end;

end $$;

