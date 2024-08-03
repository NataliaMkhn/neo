-- SCHEMA: ds

--DROP SCHEMA IF EXISTS ds ;

CREATE SCHEMA IF NOT EXISTS ds;

-- Table: ds.ft_balance_f

DROP TABLE IF EXISTS ds.ft_balance_f;

CREATE TABLE IF NOT EXISTS ds.ft_balance_f
(
    on_date date NOT NULL,
    account_rk numeric(10,0) NOT NULL,
    currency_rk numeric(10,0),
    balance_out numeric,
    CONSTRAINT ft_balance_f_pkey PRIMARY KEY (on_date, account_rk)
);

-- Table: ds.ft_posting_f

DROP TABLE IF EXISTS ds.ft_posting_f;

CREATE TABLE IF NOT EXISTS ds.ft_posting_f
(
    oper_date date NOT NULL,
    credit_account_rk numeric(10,0) NOT NULL,
    debet_account_rk numeric(10,0) NOT NULL,
    credit_amount numeric,
    debet_amount numeric
);

-- Table: ds.md_account_d

DROP TABLE IF EXISTS ds.md_account_d;

CREATE TABLE IF NOT EXISTS ds.md_account_d
(
    data_actual_date date NOT NULL,
    data_actual_end_date date NOT NULL,
    account_rk numeric(10,0) NOT NULL,
    account_number character varying(20) NOT NULL,
    char_type character varying(1)  NOT NULL,
    currency_rk numeric(10,0) NOT NULL,
    currency_code character varying(3) NOT NULL,
    CONSTRAINT md_account_d_pkey PRIMARY KEY (data_actual_date, account_rk)
);

-- Table: ds.md_currency_d

DROP TABLE IF EXISTS ds.md_currency_d;

CREATE TABLE IF NOT EXISTS ds.md_currency_d
(
    currency_rk numeric(10,0) NOT NULL,
    data_actual_date date NOT NULL,
    data_actual_end_date date,
    currency_code character varying(3),
    code_iso_char character varying(3),
    CONSTRAINT md_currency_d_pkey PRIMARY KEY (currency_rk, data_actual_date)
);

-- Table: ds.md_exchange_rate_d

DROP TABLE IF EXISTS ds.md_exchange_rate_d;

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d
(
    data_actual_date date NOT NULL,
    data_actual_end_date date,
    currency_rk numeric(10,0) NOT NULL,
    reduced_cource numeric,
    code_iso_num character varying(3),
    CONSTRAINT md_exchange_rate_d_pkey PRIMARY KEY (data_actual_date, currency_rk)
);

-- Table: ds.md_ledger_account_s

DROP TABLE IF EXISTS ds.md_ledger_account_s;

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s
(
    chapter character(1),
    chapter_name character varying(16),
    section_number integer,
    section_name character varying(22),
    subsection_name character varying(21),
    ledger1_account integer,
    ledger1_account_name character varying(47),
    ledger_account integer NOT NULL,
    ledger_account_name character varying(153),
    characteristic character(1),
    is_resident integer,
    is_reserve integer,
    is_reserved integer,
    is_loan integer,
    is_reserved_assets integer,
    is_overdue integer,
    is_interest integer,
    pair_account character varying(5),
    start_date date NOT NULL,
    end_date date,
    is_rub_only integer,
    min_term character varying(1),
    min_term_measure character varying(1),
    max_term character varying(1),
    max_term_measure character varying(1),
    ledger_acc_full_name_translit character varying(1),
    is_revaluation character varying(1),
    is_correct character varying(1),
    CONSTRAINT md_ledger_account_s_pkey PRIMARY KEY (ledger_account, start_date)
);



-- SCHEMA: logs

--DROP SCHEMA IF EXISTS logs ;

CREATE SCHEMA IF NOT EXISTS logs;

--
CREATE SEQUENCE IF NOT EXISTS logs.seq_logs
    INCREMENT 1
    START 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;


DROP TABLE IF EXISTS logs.logs;

CREATE TABLE IF NOT EXISTS logs.logs
(
	log_id INTEGER DEFAULT nextval('logs.seq_logs'::regclass),
	date_time TIMESTAMP without time zone,
        log_type VARCHAR(10),
	log_message VARCHAR
);


-- SCHEMA: dm

--DROP SCHEMA IF EXISTS dm ;

CREATE SCHEMA IF NOT EXISTS dm;

-- Table: dm.dm_account_balance_f

DROP TABLE IF EXISTS dm.dm_account_balance_f;

CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f
(
    on_date date,
    account_rk numeric(10,0),
    balance_out numeric(23,8),
    balance_out_rub numeric(23,8)
);

-- Table: dm.dm_account_turnover_f

DROP TABLE IF EXISTS dm.dm_account_turnover_f;

CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f
(
    on_date date,
    account_rk numeric(10,0),
    credit_amount numeric(23,8),
    credit_amount_rub numeric(23,8),
    debet_amount numeric(23,8),
    debet_amount_rub numeric(23,8)
);

-- Table: dm.dm_f101_round_f

DROP TABLE IF EXISTS dm.dm_f101_round_f;

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f
(
    from_date date,
    to_date date,
    chapter character(1),
    ledger_account character(5),
    characteristic character(1),
    balance_in_rub numeric(23,8),
    r_balance_in_rub numeric(23,8),
    balance_in_val numeric(23,8),
    r_balance_in_val numeric(23,8),
    balance_in_total numeric(23,8),
    r_balance_in_total numeric(23,8),
    turn_deb_rub numeric(23,8),
    r_turn_deb_rub numeric(23,8),
    turn_deb_val numeric(23,8),
    r_turn_deb_val numeric(23,8),
    turn_deb_total numeric(23,8),
    r_turn_deb_total numeric(23,8),
    turn_cre_rub numeric(23,8),
    r_turn_cre_rub numeric(23,8),
    turn_cre_val numeric(23,8),
    r_turn_cre_val numeric(23,8),
    turn_cre_total numeric(23,8),
    r_turn_cre_total numeric(23,8),
    balance_out_rub numeric(23,8),
    r_balance_out_rub numeric(23,8),
    balance_out_val numeric(23,8),
    r_balance_out_val numeric(23,8),
    balance_out_total numeric(23,8),
    r_balance_out_total numeric(23,8)
);

-- PROCEDURE: logs.log_write(character varying, character varying)

DROP PROCEDURE IF EXISTS logs.log_write(character varying, character varying);

CREATE OR REPLACE PROCEDURE logs.log_write(
	IN arg_log_type character varying(10),
	IN arg_log_message character varying)
LANGUAGE 'plpgsql'
AS $BODY$
begin
        insert into logs.logs
               values(default, now() at time zone 'MSK', arg_log_type, arg_log_message);
end; 
$BODY$;

TRUNCATE TABLE logs.logs;

INSERT INTO logs.logs VALUES (setval('logs.seq_logs', 1), now() at time zone 'MSK', 'info','созданы таблицы для загрузки данных');
