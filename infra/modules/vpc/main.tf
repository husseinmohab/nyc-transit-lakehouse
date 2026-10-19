# Network for the MSK cluster and the clients that produce into it.
#
# MSK brokers have no public endpoint when SASL/IAM is the only auth method, so
# anything that talks to Kafka has to live in this VPC.
#
# There is no NAT gateway. It runs $0.045/hour plus $0.045/GB and nothing in the
# private subnets needs to reach the internet. The brokers only serve clients
# inside the VPC and MSK ships its own broker logs through the service. The
# workloads that do need egress, polling the MTA feeds and pulling jars, run in
# the public subnets with a public IP.

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "this" {
  cidr_block = var.cidr_block

  # MSK hands clients DNS names for the bootstrap brokers, so hostnames have to
  # resolve inside the VPC or every client fails at connect time.
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(var.tags, { Name = "${var.name_prefix}-vpc" })
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = merge(var.tags, { Name = "${var.name_prefix}-igw" })
}

# Public subnets: egress-capable, for the producer and the Spark job.
resource "aws_subnet" "public" {
  count = var.az_count

  vpc_id            = aws_vpc.this.id
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = cidrsubnet(var.cidr_block, 8, count.index)

  map_public_ip_on_launch = true

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-public-${data.aws_availability_zones.available.names[count.index]}"
    Tier = "public"
  })
}

# Private subnets: the brokers. No route to the internet gateway at all, which
# is what makes "the brokers are not reachable from outside" a property of the
# routing table, not a claim about a security group.
resource "aws_subnet" "private" {
  count = var.az_count

  vpc_id            = aws_vpc.this.id
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = cidrsubnet(var.cidr_block, 8, count.index + 10)

  tags = merge(var.tags, {
    Name = "${var.name_prefix}-private-${data.aws_availability_zones.available.names[count.index]}"
    Tier = "private"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  tags   = merge(var.tags, { Name = "${var.name_prefix}-public" })
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  count = var.az_count

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# One private route table with only the implicit local route. Kept explicit
# rather than falling back to the VPC main route table, so that a later edit to
# the main table cannot accidentally give the brokers an egress path.
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = merge(var.tags, { Name = "${var.name_prefix}-private" })
}

resource "aws_route_table_association" "private" {
  count = var.az_count

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}
