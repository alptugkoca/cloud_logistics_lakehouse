variable "snowflake_account" {
  type        = string
  description = "Snowflake account identifier"
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake username"
}

variable "snowflake_password" {
  type        = string
  description = "Snowflake password"
  sensitive   = true
}
