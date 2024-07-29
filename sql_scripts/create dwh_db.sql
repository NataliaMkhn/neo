-- Database: dwh_db

-- DROP DATABASE IF EXISTS dwh_db;

CREATE DATABASE dwh_db
    WITH
    OWNER = dwh_user
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;