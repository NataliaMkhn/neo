create or replace procedure ds.fill_account_turnove_f(
                                                      in i_OnDate date
                                                      )
language 'plpgsql'
AS $$
declare

    log_txt text;
    err_code text;
    msg_text text;

begin

    log_txt =  concat('запущена процедура расчета витрины оборотов dm.dm_account_turnover_f за ', i_OnDate);
    call logs.log_write('info', log_txt);

    begin   --блок для отслеживания ошибки

        log_txt =  concat('   удаление записей за ', i_OnDate);
        call logs.log_write('info', log_txt);

        delete from dm.dm_account_turnover_f 
               where dm.dm_account_turnover_f.on_date = i_OnDate;

        log_txt =  concat('   расчет витрины оборотов за ', i_OnDate);
        call logs.log_write('info', log_txt);
   
        --расчет витрины dm_account_turnover_f
        insert into dm.dm_account_turnover_f
                             (on_date,
                              account_rk,
                              credit_amount,
                              credit_amount_rub,
                              debet_amount,
                              debet_amount_rub
                              )
        with c_query as (
                  select currency_rk, reduced_cource
                  from ds.md_exchange_rate_d
                  where i_OnDate between ds.md_exchange_rate_d.data_actual_date and 
                                         ds.md_exchange_rate_d.data_actual_end_date
                         ),
             t_query as (
                  select ds.ft_posting_f.credit_account_rk as account_rk,
                         ds.ft_posting_f.credit_amount,
                         ds.ft_posting_f.credit_amount * coalesce(c_query.reduced_cource, 1) as credit_amount_rub,
                         null as debet_amount,
                         null as debet_amount_rub
                  from ds.ft_posting_f
                  inner join ds.md_account_d
                        on ds.md_account_d.account_rk = ds.ft_posting_f.credit_account_rk
                  left  join c_query
                        on ds.md_account_d.currency_rk = c_query.currency_rk
                  where ds.ft_posting_f.oper_date = i_OnDate and
                        i_OnDate between ds.md_account_d.data_actual_date and 
                                         ds.md_account_d.data_actual_end_date

                  union all

                  select ds.ft_posting_f.debet_account_rk as account_rk,
                        null as credit_amount,
                        null as credit_amount_rub,
                        ds.ft_posting_f.debet_amount,
                        ds.ft_posting_f.debet_amount * coalesce(c_query.reduced_cource, 1) as debet_amount_rub
                  from ds.ft_posting_f 
                  inner join ds.md_account_d
                        on ds.md_account_d.account_rk = ds.ft_posting_f.debet_account_rk
                  left  join c_query
                        on ds.md_account_d.currency_rk = c_query.currency_rk
                  where ds.ft_posting_f.oper_date = i_OnDate and
                        i_OnDate between ds.md_account_d.data_actual_date and 
                                         ds.md_account_d.data_actual_end_date
                        )

        select i_OnDate as on_date,
               t_query.account_rk,
               sum(t_query.credit_amount) as credit_amount,
               sum(t_query.credit_amount_rub) as credit_amount_rub,
               sum(t_query.debet_amount) as debet_amount,
               sum(t_query.debet_amount_rub) as debet_amount_rub
        from t_query
        group by t_query.account_rk;

        log_txt =  concat('расчет витрины оборотов завершен успешно');
        call logs.log_write('info', log_txt);

    exception 
        when others
             then 

                 get stacked diagnostics
                     err_code = returned_sqlstate,
                     msg_text = message_text;

                 log_txt =  concat('   расчет витрины dm_account_turnover_f за ', i_OnDate::text, 
                                   ' завершен ошибкой:  ', err_code, ' ', msg_text);

                 call logs.log_write('error', log_txt);  
    end;

end; $$ 
;

create or replace procedure ds.fill_account_balance_f(
                                                      in i_OnDate date
                                                      )
language 'plpgsql'
AS $$
declare

    log_txt text;
    err_code text;
    msg_text text;

begin

    log_txt =  concat('запущена процедура расчета витрины остатков dm.dm_account_balance_f за ', i_OnDate);
    call logs.log_write('info', log_txt);

    begin   --блок для отслеживания ошибки

        log_txt =  concat('   удаление записей за ', i_OnDate);
        call logs.log_write('info', log_txt);

        delete from dm.dm_account_balance_f 
               where dm.dm_account_balance_f.on_date = i_OnDate;

        log_txt =  concat('   расчет витрины остатков за ', i_OnDate);
        call logs.log_write('info', log_txt);
   
        --расчет витрины dm_account_balance_f
        insert into dm.dm_account_balance_f
                             (on_date,
                              account_rk,
                              balance_out,
			                  balance_out_rub
                              )
		with t_query as (
                 select * from dm.dm_account_turnover_f 
                 where on_date = i_OnDate 
	                     ),
             b_query as (
                 select * from dm.dm_account_balance_f 
                 where on_date = i_OnDate - interval '1 day'
                         ),
	         a_query as (
                 select * from ds.md_account_d 
                 where i_OnDate between ds.md_account_d.data_actual_date and 
                                                                 ds.md_account_d.data_actual_end_date
                         ),
	         tt_query as (
                 select i_OnDate as on_date,
	                    a_query.account_rk, 
                        case a_query.char_type
                            when 'А' then 1
                            when 'П' then -1
                            else 0
                        end as act_pass,
                        coalesce(balance_out, 0) as balance_out, 
                        coalesce(balance_out_rub, 0) as balance_out_rub,
                        coalesce(credit_amount, 0) as credit_amount,
                        coalesce(credit_amount_rub, 0) as credit_amount_rub,
                        coalesce(debet_amount, 0) as debet_amount,
                        coalesce(debet_amount_rub, 0) as debet_amount_rub
                 from a_query
                 left join t_query 
                      using(account_rk)
                 left join b_query
                      using(account_rk)
	                     )
        select on_date,
	           account_rk, 
	           balance_out + act_pass * debet_amount - act_pass * credit_amount as balance_out,
	           balance_out_rub + act_pass * debet_amount_rub - act_pass * credit_amount_rub as balance_out_rub
        from tt_query;

        log_txt =  concat('расчет витрины остатков завершен успешно');
        call logs.log_write('info', log_txt);

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

end; $$ 
 
