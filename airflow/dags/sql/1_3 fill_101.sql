-- задача 1_3 расчет формы 101
create or replace procedure dm.fill_f101_round_f(
                                                  i_OnDate date
                                                 )
language 'plpgsql'
AS $$
declare

    v_start_date date;
    v_end_date date;
    v_in_date date;

    log_txt text;
    err_code text;
    msg_text text;

begin
    log_txt =  ('РАСЧЕТ ФОРМЫ 101');
    call logs.log_write('info', log_txt);

    begin   --блок для отслеживания ошибки

        log_txt = concat('  запущен расчет формы 101 на ', i_OnDate);
        call logs.log_write('info', log_txt);

        -- определяем диапазон дат для расчета витрины, входящая дата - первое число месяца, следующего за отчетным
        v_start_date = i_OnDate - interval '1 month';   -- дата начала месяца для расчета
        v_end_date = i_OnDate - interval '1 day';       -- дата конца месяца для расчета
        v_in_date = v_start_date - interval '1 day';    -- дата для входящих остатков

        delete from dm.dm_f101_round_f
        where from_date = v_start_date and to_date = v_end_date;


        insert into dm.dm_f101_round_f
                                      (from_date,
                                       to_date,
                                       chapter,
                                       ledger_account,
                                       characteristic,
                                       balance_in_rub,
                                       balance_in_val,
                                       balance_in_total,
                                       turn_deb_rub,
                                       turn_deb_val,
                                       turn_deb_total,
                                       turn_cre_rub,
                                       turn_cre_val,
                                       turn_cre_total,
                                       balance_out_rub,
                                       balance_out_val,
                                       balance_out_total
                                       )
        with in_out_query as (
                select account_rk, 
                       sum(case on_date 
                              when v_in_date then balance_out_rub
                              else 0 
                           end) as balance_in_rub,
                       sum(case on_date 
                              when v_end_date then balance_out_rub
                              else 0 
                           end) as balance_out_rub
                from dm.dm_account_balance_f
                where on_date = v_in_date or on_date = v_end_date
                group by account_rk
                             ),
             t_query as (
                select account_rk, 
                       sum(credit_amount_rub) as credit_amount_rub,
                       sum(debet_amount_rub) as debet_amount_rub
                from dm.dm_account_turnover_f
                where on_date between v_start_date and v_end_date
                group by account_rk
                        ),
             a_query as (
                select q_a.account_rk,
                       q_l.chapter,
                       q_l.ledger_account,
                       q_a.char_type,
                       q_a.currency_code
                from 
                    (    
                     select account_rk, 
                            currency_code, 
                            char_type,  
                            to_number(substring(account_number from 1 for 5), '99999') as ledger_account
                     from ds.md_account_d
                     where data_actual_date <= v_start_date and 
                           data_actual_end_date >= v_end_date
                    ) as q_a
                inner join 
                    (
					 select chapter,
                            ledger_account
                     from ds.md_ledger_account_s
                     where start_date <= v_start_date and 
                           end_date >= v_end_date
                    ) as q_l
                on q_a.ledger_account = q_l.ledger_account
                        )
        select 
               v_start_date as from_date,
               v_end_date as to_date,
               a_query.chapter,
               a_query.ledger_account,
               a_query.char_type as characteristic,
    
               sum(case 
                       when a_query.currency_code in ('810', '643') then in_out_query.balance_in_rub
                       else 0
                   end) as balabce_in_rub,
               sum(case 
                       when a_query.currency_code not in ('810', '643') then in_out_query.balance_in_rub
                       else 0
                   end) as balabce_in_val,
               sum(in_out_query.balance_in_rub) as balance_in_total,
    
               sum(coalesce(case 
                               when a_query.currency_code in ('810', '643') then t_query.debet_amount_rub
                               else 0
                            end, 0)) as turn_deb_rub,
               sum(coalesce(case 
                               when a_query.currency_code not in ('810', '643') then t_query.debet_amount_rub
                               else 0
                            end, 0)) as turn_deb_val,
               sum(coalesce(t_query.debet_amount_rub, 0)) as turn_deb_total,
    
               sum(coalesce(case 
                               when a_query.currency_code in ('810', '643') then t_query.credit_amount_rub
                               else 0
                            end, 0)) as turn_cre_rub,
               sum(coalesce(case 
                               when a_query.currency_code not in ('810', '643') then t_query.credit_amount_rub
                               else 0
                            end, 0)) as turn_cre_val,
               sum(coalesce(t_query.credit_amount_rub, 0)) as turn_cre_total,
    
               sum(case 
                       when a_query.currency_code in ('810', '643') then in_out_query.balance_out_rub
                       else 0
                   end) as balabce_out_rub,
               sum(case 
                       when a_query.currency_code not in ('810', '643') then in_out_query.balance_out_rub
                       else 0
                   end) as balabce_out_val,
               sum(in_out_query.balance_out_rub) as balance_out_total    
    
        from a_query
        left join in_out_query
             using(account_rk)
        left join t_query
             using (account_rk)
        group by from_date,
                 to_date,
                 chapter,
                 ledger_account,
                 characteristic;

        log_txt =  concat('  расчет формы 101 на ', i_OnDate, ' завершен успешно');
        call logs.log_write('info', log_txt);

    exception 
        when others
             then 

                 get stacked diagnostics
                     err_code = returned_sqlstate,
                     msg_text = message_text;

                 log_txt =  concat('   расчет ajhvs 101 за ', i_OnDate, 
                                   ' завершен ошибкой:  ', err_code, ' ', msg_text);

                 call logs.log_write('error', log_txt);  
    end;

end $$;
