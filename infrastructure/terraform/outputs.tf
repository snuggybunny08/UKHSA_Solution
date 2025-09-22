
output "s3_buckets" {
  description = "S3 bucket names"
  value = {
    landing        = aws_s3_bucket.landing.id
    validated      = aws_s3_bucket.validated.id
    archive        = aws_s3_bucket.archive.id
    validation_logs = aws_s3_bucket.validation_logs.id
    athena_results = aws_s3_bucket.athena_results.id
    glue_scripts   = aws_s3_bucket.glue_scripts.id
  }
}

output "lambda_functions" {
  description = "Lambda function details"
  value = {
    file_processor = {
      name = aws_lambda_function.file_processor.function_name
      arn  = aws_lambda_function.file_processor.arn
    }
    data_validator = {
      name = aws_lambda_function.data_validator.function_name
      arn  = aws_lambda_function.data_validator.arn
    }
  }
}

output "glue_resources" {
  description = "Glue resource details"
  value = {
    database = aws_glue_catalog_database.main.name
    crawler  = aws_glue_crawler.validated_data.name
    etl_job  = aws_glue_job.etl_job.name
  }
}

output "sns_topics" {
  description = "SNS topic ARNs"
  value = {
    validation_alerts = aws_sns_topic.validation_alerts.arn
    system_alerts    = aws_sns_topic.alerts.arn
  }
}

output "dynamodb_tables" {
  description = "DynamoDB table names"
  value = {
    validation_rules = aws_dynamodb_table.validation_rules.name
  }
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.main.name
}

output "iam_roles" {
  description = "IAM role ARNs"
  value = {
    lambda_role = aws_iam_role.lambda_role.arn
    glue_role   = aws_iam_role.glue_role.arn
  }
}


