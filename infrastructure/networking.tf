# Get a list of all available availability zones in the current region
data "aws_availability_zones" "available" {
  state = "available"
}

# Create a new VPC for our Airflow environment
resource "aws_vpc" "mwaa_vpc" {
  cidr_block = "10.199.0.0/16"
  tags = {
    Name = "sentinel-mwaa-vpc"
  }
}

# Create two public subnets for the NAT Gateway
resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.mwaa_vpc.id
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = "10.199.${10 + count.index}.0/24"
  map_public_ip_on_launch = true
  tags = {
    Name = "sentinel-mwaa-public-subnet-${count.index + 1}"
  }
}

# Create two private subnets where Airflow will run
resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.mwaa_vpc.id
  availability_zone = data.aws_availability_zones.available.names[count.index]
  cidr_block        = "10.199.${20 + count.index}.0/24"
  tags = {
    Name = "sentinel-mwaa-private-subnet-${count.index + 1}"
  }
}

# Create an Internet Gateway to allow access to the internet from our VPC
resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.mwaa_vpc.id
  tags = {
    Name = "sentinel-mwaa-igw"
  }
}

# Create a Route Table for the public subnets to route traffic to the Internet Gateway
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.mwaa_vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
  tags = {
    Name = "sentinel-mwaa-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# Create a NAT Gateway to allow our private subnets to access the internet
resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags = {
    Name = "sentinel-mwaa-nat"
  }
  depends_on = [aws_internet_gateway.gw]
}

# Create a Route Table for the private subnets to route traffic to the NAT Gateway
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.mwaa_vpc.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }
  tags = {
    Name = "sentinel-mwaa-private-rt"
  }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}