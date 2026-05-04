terraform {
  required_version = ">= 1.5.0"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.95"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  user     = var.snowflake_user
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

resource "snowflake_database" "logistics_db" {
  name = "LOGISTICS_DB"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.logistics_db.name
  name     = "GOLD"
}

resource "snowflake_warehouse" "compute_wh" {
  name           = "COMPUTE_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
}
