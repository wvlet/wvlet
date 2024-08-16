package com.treasuredata.flow.lang.compiler

enum DBType:
  case DuckDB
  case Trino
  case Hive
  case BigQuery
  case MySQL
  case PostgreSQL
  case SQLite
  case Redshift
  case Snowflake
  case ClickHouse
  case Oracle
  case SQLServer
  case InMemory
  case Other(name: String)
