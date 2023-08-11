# AWS Glue ETL Boilerplate

Welcome to the AWS Glue ETL Boilerplate repository! This is an example AWS Glue application that uses the Serverless Framework to deploy infrastructure and allows local development with AWS Glue Libs, Spark, Jupyter Notebook, and more. It includes jobs using Python Shell and PySpark.

<div align="center">

```ocaml
CLICK OR TAP ❲☰❳ TO SHOW TABLE-OF-CONTENTS :D
```

</div> <!-- center -->

## Motivation 🚀

Are you ready to supercharge your ETL development with AWS Glue? This repository is here to help you quickly set up, develop, and deploy AWS Glue jobs. Streamline your ETL pipelines, harness the power of AWS Glue Libs and Spark, and unlock efficient local development.

Check out the [Use Case Scenario](#use-case-scenario) to learn more about the motivation behind this example!

## Features ✨

- **Full AWS Glue Setup:** Deploy Glue jobs using Python Shell Script and PySpark.
- **Flexible Local Development:** Choose between using VSCode + Remote Containers or Docker Compose.
- **Comprehensive Documentation:** Easy-to-follow guides for development and deployment.
- **Reusable Examples:** Building upon multiple examples to provide a well-rounded solution.
- **Serverless Framework:** Utilize Serverless Framework to deploy AWS Glue jobs and other resources.

## Usage

To quickly start a project using this example, follow these steps:

```sh
npx serverless install -u https://github.com/nanlabs/aws-glue-etl-boilerplate -n my-project
```

## Overview

This boilerplate was created by combining the best practices from our following examples:

- [Serverless Glue example](https://github.com/nanlabs/devops-reference/tree/main/examples/serverless-glue/) - Deploy AWS Glue jobs using the Serverless Framework.
- [AWS Glue docker example](https://github.com/nanlabs/devops-reference/tree/main/examples/compose-glue/) - Run AWS Glue jobs locally using Docker Compose.
- [VSCode DevContainer example](https://github.com/nanlabs/devops-reference/tree/main/examples/devcontainer-glue/) - Run AWS Glue jobs locally using VSCode + Remote Containers.

## Requirements

- [Docker](https://www.docker.com/)
- [VSCode](https://code.visualstudio.com/) (optional)

## Local Development

Choose your preferred local development setup!

### Using VSCode + Remote Containers (recommended)

1. Install Docker
2. Install [VSCode](https://code.visualstudio.com/)
3. Install the [Remote Development](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.vscode-remote-extensionpack) extension
4. Clone this repository
5. Create your application within a container (see gif below)

![Create application within a container](./docs/vscode-open-in-container.gif)

Once the container is running inside VSCode, you can run the Glue jobs locally as follows:

```sh
# Set AWS credentials with dummy values to run locally
export AWS_ACCESS_KEY_ID="dummyaccess"
export AWS_SECRET_ACCESS_KEY="dummysecret"

# Run PySpark job
glue-spark-submit jobs/pyspark_hello_world.py --JOB_NAME job_example --CUSTOM_ARGUMENT custom_value
```

### Using Docker Compose manually

Refer to the [development documentation](./docs/DEVELOPMENT.md) for detailed steps to set up a local development environment using Docker Compose.

## Deployment

We utilize the Serverless Framework to deploy AWS Glue jobs and other resources. For deployment instructions, check out the [deployment documentation](./docs/DEPLOYMENT.md).

## Implementation Notes

You can find detailed implementation notes in the [Implementation Notes](./docs/IMPLEMENTATION_NOTES.md) document.

---

## Use Case Scenario

> Empowering Threat Intelligence with our AWS Glue ETL Boilerplate

Imagine the scenario:

**Objective:** Your organization is on a mission to bolster its threat intelligence capabilities by creating a robust datalake that aggregates and analyzes data from various Open Source Intelligence (OSINT) sources. The goal is to enhance security operations and proactively identify potential threats.

**Challenge:** Traditional threat intelligence methods lack the agility and scalability needed to process the massive influx of data from OSINT sources. Manual data collection and analysis are time-consuming, making it difficult to stay ahead of emerging threats.

**Solution:** Introducing our AWS Glue ETL Boilerplate – a cutting-edge solution that harnesses the power of AWS Glue, Serverless Framework, and efficient local development techniques. This comprehensive example demonstrates how to build an end-to-end datalake tailored for threat intelligence operations.

**Key Features and Benefits:**

🔒 **Enhanced Security Operations:** By centralizing data from OSINT sources, your security team gains a consolidated view of potential threats. Real-time analysis enables quicker responses to emerging incidents.

⚙️ **Flexible ETL Infrastructure:** The Serverless Framework empowers you to deploy AWS Glue jobs seamlessly, adapting to varying data sources and formats. This flexibility ensures smooth data integration.

💡 **Efficient Local Development:** Develop and refine your threat intelligence pipeline locally using VSCode + Remote Containers or Docker Compose. Rapid iteration and testing significantly expedite deployment.

📈 **Scalability for Data Growth:** As your OSINT data volume expands, the solution effortlessly scales to accommodate increasing demands. This ensures your threat intelligence efforts remain effective and up-to-date.

📚 **Comprehensive Documentation:** A wealth of documentation guides your team through each step – from initial setup to deployment – ensuring successful implementation.
