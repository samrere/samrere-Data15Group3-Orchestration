#!/bin/sh
# Get the SMTP username and password from secrets manager
username=$(aws secretsmanager get-secret-value --secret-id airflow/smtp.smtp_user --query SecretString --output text)
password=$(aws secretsmanager get-secret-value --secret-id airflow/smtp.smtp_password --query SecretString --output text)
fernet_key=$(aws secretsmanager get-secret-value --secret-id airflow/core.fernet_key --query SecretString --output text)

# encryption for variables, connections, and xcoms
export AIRFLOW__CORE__FERNET_KEY=$fernet_key

# email
export AIRFLOW__SMTP__SMTP_USER=$username
export AIRFLOW__SMTP__SMTP_PASSWORD=$password
export AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
export AIRFLOW__SMTP__SMTP_PORT=587
export AIRFLOW__SMTP__SMTP_SSL=False
export AIRFLOW__SMTP__SMTP_STARTTLS=True



echo "SMTP user is $AIRFLOW__SMTP__SMTP_USER"
echo "Fernet Key has been set."
