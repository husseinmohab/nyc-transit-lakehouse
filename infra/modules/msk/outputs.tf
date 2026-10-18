output "cluster_arn" {
  value = aws_msk_cluster.this.arn
}

output "cluster_name" {
  value = aws_msk_cluster.this.cluster_name
}

output "bootstrap_brokers_sasl_iam" {
  value     = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
  sensitive = true
}

output "security_group_id" {
  value = aws_security_group.msk.id
}

output "configuration_arn" {
  value = aws_msk_configuration.this.arn
}

output "topic_script_path" {
  description = "Generated script that creates the declared topics."
  value       = local_file.topic_script.filename
}
