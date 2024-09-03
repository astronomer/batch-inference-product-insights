## Batch inference for product insights - Reference architecture

Welcome! 🚀

This project is an end-to-end pipeline showing how to implement batch inference for product insights using [Apache Airflow®](https://airflow.apache.org/) and [OpenAI](https://openai.com/). You can use this project as a starting point to build your own pipelines for similar use cases.

> [!TIP]
> If you are new to Airflow, we recommend checking out our get started resources: [DAG writing for data engineers and data scientists](https://www.astronomer.io/events/webinars/dag-writing-for-data-engineers-and-data-scientists-video/) before diving into this project.

## Tools used

- [Apache Airflow®](https://airflow.apache.org/docs/apache-airflow/stable/index.html) running on [Astro](https://www.astronomer.io/product/). A [free trial](http://qrco.de/bfHv2Q) is available.
- [Amazon S3](https://aws.amazon.com/s3/) free tier. You can also adapt the pipeline to run with your preferred object storage solution.
- [OpenAI](https://platform.openai.com/docs/overview) to create embedding vectors. You need at least a Tier 1 key to run the project.
- A [GitHub](https://docs.github.com/en/get-started/start-your-journey/creating-an-account-on-github) account is needed to create a GH API token.
- A [Slack](https://slack.com/) workspace with permissions to add a new app is needed for the Slack notification tasks.

Optional:

One of the two ingestion DAGs ([`ingest_zendesk_tickets`](/dags/ingest_zendesk_tickets.py)) fetches data from a Snowflake table. If you want to run this DAG, you will need:

- [Snowflake](https://www.snowflake.com/en/). A [free trial](https://signup.snowflake.com/) is available.

## How to setup the demo environment

Follow the steps below to set up the demo for yourself.

1. Install Astronomer's open-source local Airflow development tool, the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview).
2. Log into your AWS account and create [a new empty S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html). Make sure you have a set of [AWS credentials](https://docs.aws.amazon.com/iam/) with `AmazonS3FullAccess` for this new bucket.
3. (Optional): Sign up for a [free trial](https://trial.snowflake.com/?owner=SPN-PID-365384) of Snowflake. Create a database called `product_insights_demo` with a schema called `dev` and a table called `zd_tickets`. If you do not want to use Snowflake, delete the `ingest_zendesk_tickets` DAG from the `dags` folder.

    The table needs to contain at least the following columns: `ticket_id`, `subject`, `description`. You can use the following SQL to create the table:
        
        ```sql
        create or replace TABLE PRODUCT_INSIGHTS_DEMO.DEV.ZD_TICKETS (
            TICKET_ID NUMBER(38,0) NOT NULL COMMENT 'Zendesk ticket unique id. Primary key of the table.',
            SUBJECT VARCHAR(512) COMMENT 'Subject of the ticket. Indicated by the ticket submitter.',
            DESCRIPTION VARCHAR(65536) COMMENT 'Ticket description. Indicated by the ticket submitter.',
        );
        ``` 

    And the following statement to populate it with mock data:

        ```sql
        insert into PRODUCT_INSIGHTS_DEMO.DEV.ZD_TICKETS (TICKET_ID, SUBJECT, DESCRIPTION)
        values
        (1, 'Issue with login', 'I cannot login from my phone'),
        (2, 'Feedback', 'Thanks for resolving my problem so quickly! Also I love the new UI'),
        (3, 'Feature request', 'I would like to be able to change my password from the mobile app')
        ```

4. Fork this repository and clone the code locally.

### Run the project locally

1. Create a new file called `.env` in the root of the cloned repository and copy the contents of [.env_example](.env_example) into it. Fill out the placeholders with your own credentials for OpenAI, AWS, your [Slack app](https://api.slack.com/docs/apps), the [GitHub API](https://docs.github.com/en/rest/authentication/authenticating-to-the-rest-api) and optionally, Snowflake. Also make sure to add your S3 bucket name.

The demo pipeline ingests information about Apache Airflow and Astro and will look for feedback related to the features and products listed in `CLASSES`. You can modify this list to include your other products or features.

2. In the root of the repository, run `astro dev start` to start up the following Docker containers. This is your local development environment.

    - Postgres: Airflow's Metadata Database.
    - Webserver: The Airflow component responsible for rendering the Airflow UI. Accessible on port `localhost:8080`.
    - Scheduler: The Airflow component responsible for monitoring and triggering tasks
    - Triggerer: The Airflow component responsible for triggering deferred tasks

    Note that after any changes to `.env` you will need to run `astro dev restart` for new environment variables to be picked up.

3. Access the Airflow UI at `localhost:8080` and follow the DAG running instructions in the [Running the DAGs](#running-the-dags) section of this README.

### Run the project in the cloud

1. Sign up to [Astro](https://www.astronomer.io/try-astro/?utm_source=learn-docs-reference-architectures&utm_medium=web&utm_campaign=free-trial) for free and follow the onboarding flow to create a deployment with default configurations.
2. Deploy the project to Astro using `astro deploy`. See [Deploy code to Astro](https://www.astronomer.io/docs/astro/deploy-code).
3. Set up your Slack, AWS and Snowflake connections, as well as all other environment variables listed in [`.env_example](.env_example) on Astro. For instructions see [Manage Airflow connections and variables](https://www.astronomer.io/docs/astro/manage-connections-variables) and [Manage environment variables on Astro](https://www.astronomer.io/docs/astro/manage-env-vars).
4. Open the Airflow UI of your Astro deployment and follow the steps in [Running the DAGs](#running-the-dags).

## Running the DAGs

1. Unpause all DAGs in the Airflow UI by clicking the toggle to the left of the DAG name.
2. Run either the `ingest_zendesk_tickets` or the `ingest_data_apis` DAG manually to start the data-driven pipelines. When running `ingest_data_apis` you will see a form to select with APIs to ingest data from, you need to select at least one to proceed.
3. Wait until the DAGs finish running. If you have set up the Slack connection, you will receive notifications with the generated insights.

## Next steps

Get the [Astronomer GenAI cookbook](https://www.astronomer.io/ebooks/gen-ai-airflow-cookbook/?utm_source=learn-docs-reference-architectures&utm_medium=web&utm_campaign=gen-ai) to view more examples of how to use Airflow to build generative AI applications.

If you'd like to build your own batch inference pipeline, feel free adapt this repository to your use case. We recommend to deploy the Airflow pipelines using a [free trial](https://www.astronomer.io/try-astro/?utm_source=learn-docs-reference-architectures&utm_medium=web&utm_campaign=free-trial) of Astro.