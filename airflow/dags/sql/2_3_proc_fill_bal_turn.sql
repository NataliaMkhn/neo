/* 4) Написать процедуру по аналогии с заданием 2.2 для перезагрузки данных в витрину */
create or replace procedure dm.fill_account_balance_turnover()
language 'plpgsql'
AS $$
begin	

    truncate table dm.account_balance_turnover;

    insert into dm.account_balance_turnover
    select a.account_rk
		 , coalesce(dc.currency_name, '-1'::TEXT) as currency_name
	     , a.department_rk
	     , ab.effective_date
         , ab.account_in_sum
         , ab.account_out_sum
    from rd.account a
    left join rd.account_balance ab on a.account_rk = ab.account_rk
    left join dm.dict_currency dc on a.currency_cd = dc.currency_cd;

end; $$ 
;