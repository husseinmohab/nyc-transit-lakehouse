output "database_names" {
  value = [for db in aws_glue_catalog_database.layers : db.name]
}

output "database_arns" {
  value = [for db in aws_glue_catalog_database.layers : db.arn]
}

output "catalog_id" {
  value = data.aws_caller_identity.current.account_id
}
