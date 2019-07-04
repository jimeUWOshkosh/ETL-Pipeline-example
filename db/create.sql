/*
 * create.sql
 * Copyright (C) 2018 James Edwards <mroshvegas@gmail.com>
 *
 * Distributed under terms of the License license.
 */

DROP TABLE IF EXISTS dataset;
CREATE TABLE IF NOT EXISTS dataset (
       file_id INTEGER PRIMARY KEY AUTOINCREMENT,
       file    TEXT not null,
       transaction_date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       unique (file_id)
);

DROP TABLE IF EXISTS datasheet;
CREATE TABLE IF NOT EXISTS datasheet (
       sheet_id   INTEGER PRIMARY KEY AUTOINCREMENT,
       file_id    INTEGER not null references dataset(file_id),
       sheet_indx INTEGER not null,
       sheet_name TEXT    not null,
       unique (sheet_id),
       unique (file_id, sheet_indx)
);

DROP TABLE IF EXISTS data;
CREATE TABLE IF NOT EXISTS data (
       data_id   INTEGER PRIMARY KEY AUTOINCREMENT,
       file_id  INTEGER not null references dataset(file_id),
       sheet_id INTEGER not null references datasheet(sheet_id),
       row_indx INTEGER not null,
       name     TEXT    not null,
       age      INTEGER not null,
       utf      TEXT    not null,
       unique (data_id),
       unique (file_id, sheet_id, row_indx)
);
