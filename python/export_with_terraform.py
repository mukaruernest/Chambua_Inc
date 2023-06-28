from python_terraform import Terraform
# import provider configurations
from config import aws_region, host, port, username, password
# Initialize Terraform configuration
tf = Terraform(working_dir='../chambua_inc')

# Define the table names
aggregates = ["agg_shipments", "agg_public_holiday", "agg_performing_product"]
version = ">= 1.19.0"
source = "cyrilgdn/postgresql"

# Define the Terraform configuration dynamically
configuration = f'''
terraform {{
  required_providers {{
    postgresql = {{
      version = "{version}"
      source  = "{source}"
    }}
  }}
}}
provider "aws" {{
  region = "{aws_region}"
}}

provider "postgresql" {{
  host     = "{host}"
  port     = {port}
  username = "{username}"
  password = "{password}"
}}

resource "aws_s3_bucket" "export_bucket" {{
  bucket = "your-export-bucket-name"
  acl    = "public-read"  # Adjust the ACL as per your requirements
}}

'''

# Generate the "data postgresql_table" blocks dynamically
for aggregate in aggregates:
    configuration += f'''
data "postgresql_table" "{aggregate}" {{
  schema = ""ernemuka4263_analytics""
  name   = "{aggregate}"
}}
'''

# Generate the "resource aws_s3_bucket_object" blocks dynamically
for aggregate in aggregates:
    configuration += f'''
resource "aws_s3_bucket_object" "{aggregate}_export" {{
  bucket = aws_s3_bucket.export_bucket.id
  key    = "analytics_export/ernemuka4263/{aggregate}_export.csv"
  source = data.postgresql_table.{aggregate}.query_export_csv
}}
'''

# Load and apply the Terraform configuration
# tf.load_config(configuration)
tf.init()
tf.apply(skip_plan=True)