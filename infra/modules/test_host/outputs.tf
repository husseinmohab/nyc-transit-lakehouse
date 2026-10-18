output "instance_id" {
  description = "Target for `aws ssm send-command`. There is no SSH key and no open port."
  value       = aws_instance.this.id
}

output "public_ip" {
  value = aws_instance.this.public_ip
}

output "role_arn" {
  value = aws_iam_role.this.arn
}

output "security_group_id" {
  value = aws_security_group.this.id
}

output "bootstrap_log" {
  description = "Where to look when the host comes up but the stack is not installed."
  value       = "/var/log/cloud-init-output.log"
}

output "install_dir" {
  value = local.install_dir
}
