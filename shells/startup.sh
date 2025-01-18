#!/bin/sh
# Get the SMTP username and password from secrets manager
username=$(aws secretsmanager get-secret-value --secret-id airflow/smtp.smtp_user --query SecretString --output text)
password=$(aws secretsmanager get-secret-value --secret-id airflow/smtp.smtp_password --query SecretString --output text)

# Set the SMTP Environment variables with the username and password retrieved from Secrets Manager
export AIRFLOW__SMTP__SMTP_USER=$username
export AIRFLOW__SMTP__SMTP_PASSWORD=$password

# Print the SMTP user
echo "SMTP user is $AIRFLOW__SMTP__SMTP_USER"
