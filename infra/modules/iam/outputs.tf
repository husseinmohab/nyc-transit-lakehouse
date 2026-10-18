output "producer_role_arn" {
  value = aws_iam_role.producer.arn
}

output "streaming_role_arn" {
  value = aws_iam_role.streaming.arn
}

output "dbt_role_arn" {
  value = aws_iam_role.dbt.arn
}

output "role_arns_by_workload" {
  value = {
    producer  = aws_iam_role.producer.arn
    streaming = aws_iam_role.streaming.arn
    dbt       = aws_iam_role.dbt.arn
  }
}
