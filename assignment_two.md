## Question 1. Load January 2020 dataset_url
Using the etl_web_to_gcs.py flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

**answer**:

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """ Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("data-talks-homework")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path = path, to_path=path)


@flow()
def web_to_gcs() -> None:
    '''The main ETL function'''
    color = "green"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url= f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,color,dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    web_to_gcs()
```

Number of rows in this dataset = **447,770**

## Question 2: Scheduling with Cron
Cron is a common scheduling specification for workflows.

Using the flow in etl_web_to_gcs.py, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

**answer**
```
Using same code above, we run the commands below:

prefect deployment build ./web_to_gcs.py:web_to_gcs -n "Greentaxi-ETL" --cron "0 5 1 * *"

prefect apply web_to_gcs-deployment.yaml

```
cron expression is **"0 5 1 * *"**

## Question 3: Loading data to BigQuery
Using etl_gcs_to_bq.py as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

**answer**:

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=3)
def extract_from_gcs(color: str ,year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f".")
    return Path(f"{gcs_path}")

@task(log_prints=True, retries=3)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"the path was {path}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True, retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """Write Dataframe to Biquery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="de_zoomcamp_prefect.yellow_taxi_rides",
        project_id="dtc-terraform-de",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str):
    """Main ETL flow to load data into Big Query"""
    #color="yellow"
    #year=2019
    #month=1

    path = extract_from_gcs(color,year,month)
    df = transform(path)
    write_bq(df)

@flow(log_prints=True)
def etl_parent_flow(
    months = [2, 3], year: int = 2019,  color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == '__main__':
    color = "yellow"
    months = [2,3]
    year = 2019
    etl_parent_flow(months, year, color)

```

Next we run deployment
```
prefect deployment build ./gcs_to_bq.py:etl_parent_flow -n "Greentaxi-bq_sync"

prefect apply gcs_to_bq-deployment.yaml
```

Total number of rows = **14,851,920**

## Question 4: Github Storage Block
Using the web_to_gcs script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

**answer**:

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import GitHub

@task()
def github_pull() -> None:
    """ retrieve code from github repo"""
    github_block = GitHub.load("github-access")
    github_block.get_directory(local_path="./repo/")


@flow()
def web_to_gcs() -> None:
    '''The main ETL function'''
    github_pull()

    year = 2020
    color = "green"
    month = 11

    from repo.week_2.web_to_gcs import web_to_gcs
    web_to_gcs(year, color, month)


if __name__ == '__main__':
    web_to_gcs()

```
Number of rows processed = **88605**

## 5: Email or Slack notifications
Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur.

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up.

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook.

Join my temporary Slack workspace with this link. 400 people can use this link and it expires in 90 days.

In the Prefect Cloud UI create an Automation or in the Prefect Orion UI create a Notification to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create.

How many rows were processed by the script?

**answer**:

```
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.filesystems import GitHub
from prefect.blocks.notifications import SlackWebhook


@task()
def github_pull() -> None:
    """ retrieve code from github repo"""
    github_block = GitHub.load("github-access")
    github_block.get_directory(local_path="./repo/")

@task()
def notifications() -> None:
    slack_webhook_block = SlackWebhook.load("slack-notifications")
    slack_webhook_block.notify("Flow run completed successfully!")

@flow()
def web_to_gcs() -> None:
    '''The main ETL function'''
    github_pull()

    year = 2019
    color = "green"
    month = 4

    from repo.week_2.web_to_gcs import web_to_gcs
    web_to_gcs(year, color, month)

    notifications()

if __name__ == '__main__':
    web_to_gcs()

```
total number of rows: **514,392**

## question 6: Secrets
Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

**answer**: **8** characters are shown.
