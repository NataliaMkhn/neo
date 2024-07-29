TRUNCATE TABLE dsl.ft_balance_f;

INSERT INTO dsl.ft_balance_f(
      account_rk
    , currency_rk
    , balance_out
    , on_date
)
SELECT fbf."ACCOUNT_RK" 
     , fbf."CURRENCY_RK" 
     , fbf."BALANCE_OUT" 
     , to_date(fbf."ON_DATE" , 'dd.mm.YYYY') AS on_date
  FROM stage.ft_balance_f fbf
 WHERE fbf."ACCOUNT_RK"  IS NOT NULL
   AND fbf."CURRENCY_RK"  IS NOT NULL;
