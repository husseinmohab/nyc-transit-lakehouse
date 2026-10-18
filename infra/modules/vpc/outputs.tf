output "vpc_id" {
  value = aws_vpc.this.id
}

output "vpc_cidr_block" {
  value = aws_vpc.this.cidr_block
}

output "public_subnet_ids" {
  description = "Egress-capable subnets for the producer and the Spark job."
  value       = aws_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "Subnets for the MSK brokers. No internet gateway route."
  value       = aws_subnet.private[*].id
}

output "availability_zones" {
  value = slice(data.aws_availability_zones.available.names, 0, var.az_count)
}
