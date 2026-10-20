import os
import time

import boto3
import duckdb                         # new
import requests
from dbt.cli.main import dbtRunner   # new
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()
UVA_ID = os.environ["UVA_ID"]
PORT = os.environ.get("SCATTER_PORT", "8000")
SCATTER_URL = f"http://127.0.0.1:{PORT}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")
sqs = boto3.client("sqs")   # endpoint comes from AWS_ENDPOINT_URL_SQS

@task(retries=2, retry_delay_seconds=5)
def populate_queue() -> str:
    logger = get_run_logger()
    resp = requests.post(SCATTER_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    logger.info("Scatter API answered: %s", payload)
    return payload["sqs_url"]

COUNTERS = ["ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"]

def get_counts(queue_url: str) -> dict:
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=COUNTERS)["Attributes"]
    return {"visible":   int(attrs["ApproximateNumberOfMessages"]),
            "in_flight": int(attrs["ApproximateNumberOfMessagesNotVisible"]),
            "delayed":   int(attrs["ApproximateNumberOfMessagesDelayed"])}

@task
def monitor_queue(queue_url: str, poll_s: int = 5,
                  timeout_s: int = 1200) -> dict:
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        c = get_counts(queue_url)
        logger.info("visible=%d  in_flight=%d  delayed=%d",
                    c["visible"], c["in_flight"], c["delayed"])
        if c["delayed"] == 0:
            logger.info("All messages released")
            return c
        time.sleep(poll_s)
    raise TimeoutError(f"Messages still delayed after {timeout_s} s")

@task
def prepare_landing() -> None:
    """Create an empty raw.fragments table for this run."""
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("create schema if not exists raw")
        con.execute("create or replace table raw.fragments ("
                    "order_no varchar, word varchar, "
                    "received_at timestamp)")
        con.execute("create or replace table raw.rejects ("
                    "message_id varchar, attributes varchar, "
                    "received_at timestamp)")


def parse(msg) -> tuple[str, str] | None:
    a = msg.get("MessageAttributes", {})
    try:
        order_no = a["order_no"]["StringValue"]
        word = a["word"]["StringValue"]
    except KeyError:
        return None                       # an attribute is missing
    if not order_no.isdigit():
        return None                       # position is not a number
    return order_no, word


def stored_count(con) -> int:
    """How many different fragments are stored so far."""
    return con.execute("select count(distinct order_no) "
                       "from raw.fragments").fetchone()[0]

@task
def collect_messages(queue_url: str, expected: int = 21,
                     timeout_s: int = 1200) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    with duckdb.connect(DUCKDB_PATH) as con:
        while time.monotonic() < deadline:
            stored, c = stored_count(con), get_counts(queue_url)
            logger.info("stored %d/%d | visible=%d in_flight=%d delayed=%d",
                        stored, expected, c["visible"],
                        c["in_flight"], c["delayed"])
            if stored >= expected and sum(c.values()) == 0:
                return stored
            resp = sqs.receive_message(QueueUrl=queue_url,
                                       MaxNumberOfMessages=10,
                                       MessageAttributeNames=["All"],
                                       WaitTimeSeconds=10)
            for msg in resp.get("Messages", []):
                fragment = parse(msg)
                if fragment is None:              # poison message
                    logger.warning("Rejected malformed message %s",
                                   msg["MessageId"])
                    con.execute("insert into raw.rejects "
                                "values (?, ?, current_timestamp)",
                                [msg["MessageId"],
                                 str(msg.get("MessageAttributes"))])
                else:
                    con.execute("insert into raw.fragments "
                                "values (?, ?, current_timestamp)",
                                list(fragment))
                sqs.delete_message(QueueUrl=queue_url,       # always
                                   ReceiptHandle=msg["ReceiptHandle"])
        stored = stored_count(con)
    raise TimeoutError(f"Only {stored}/{expected} fragments "
                       f"after {timeout_s} s")

@task
def dbt_build() -> None:
    """Run dbt build; raise if anything fails."""
    res = dbtRunner().invoke(["build", "--project-dir", "dbt",
                              "--profiles-dir", "dbt"])
    if not res.success:
        raise RuntimeError("dbt tests failed: not submitting")
    get_run_logger().info("dbt build passed: all tests green")


@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    with duckdb.connect(DUCKDB_PATH) as con:
        n, phrase = con.execute("select fragment_count, phrase "
                                "from assembled_phrase").fetchone()
    get_run_logger().info("Assembled %d fragments: %s", n, phrase)
    return phrase

@task(retries=2, retry_delay_seconds=5)
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes."""
    url = sqs.get_queue_url(QueueName="dp2-submit")["QueueUrl"]
    resp = sqs.send_message(
        QueueUrl=url, MessageBody="dp2 solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": UVA_ID},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": platform}})
    status = resp["ResponseMetadata"]["HTTPStatusCode"]
    get_run_logger().info("Submitted, HTTP %s", status)
    if status != 200:
        raise RuntimeError(f"Submission returned HTTP {status}")
    return status


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline(timeout_s: int = 1200):
    queue_url = populate_queue()
    prepare_landing()
    collect_messages(queue_url, timeout_s=timeout_s)
    dbt_build()
    phrase = read_phrase()
    submit_solution(phrase)


if __name__ == "__main__":
    dp2_pipeline()
