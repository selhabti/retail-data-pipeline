# Retail Data Pipeline

A complete data pipeline for generating, processing, and analyzing synthetic retail data on Google Cloud Platform.

## Architecture

- **Data Generation**: Cloud Function that generates customer, product, and order data
- **Storage**: Google Cloud Storage for raw CSV files
- **Processing**: Apache Airflow (Cloud Composer) for orchestration
- **Analytical Storage**: BigQuery with partitioned tables
- **Infrastructure**: Terraform for Infrastructure as Code

## Project Structure

retail-data-pipeline/
├── airflow/ # Airflow DAGs and plugins
├── cloud_functions/ # Cloud Functions code
├── bigquery/ # BigQuery schemas and SQL
├── terraform/ # Infrastructure as Code
└── tests/ # Unit and integration tests


## Deployment

See the [deployment guide](docs/deployment.md) for detailed instructions.

## Development

- Follow PEP 8 coding conventions
- Add unit tests for new features
- Create a branch for each new feature
- Submit pull requests for review

## License
MIT