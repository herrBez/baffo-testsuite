import json
import sys
import os
import logging
import ecs_logging
import elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
from deepdiff import DeepDiff  # Assuming deepdiff is installed
from pathlib import Path
import datetime
from time import sleep
from typing import List, Dict, Optional
import urllib3
import subprocess
from dataclasses import dataclass


@dataclass
class TestDiff:
    diff: DeepDiff
    doc_id: str
    doc_es: Optional[Dict] = None
    doc_ls: Optional[Dict] = None
    doc_original: Optional[Dict] = None


@dataclass
class TestResult:
    successes: int = 0
    errors: int = 0
    error_diffs: List[TestDiff] = None

@dataclass
class TestSuite:
    results: Dict[str, TestResult] = None
    es_version: str = None

    def add_result(self, key: str, result: TestResult):
        if self.results is None:
            self.results = {}
        self.results[key] = result


    def total_successes(self) -> int:
        return sum(result.successes for result in self.results.values()) if self.results else 0

    def total_errors(self) -> int:
        return sum(result.errors for result in self.results.values()) if self.results else 0

    def total_tests(self) -> int:
        return len(self.results) if self.results else 0

    def total_tested_documents(self) -> int:
        return self.total_successes() + self.total_errors()


# --- Helper Function ---


def generate_markdown_report(test_suite: TestSuite) -> str:
    report_lines = ["# 🧪 Baffo Test Suite Report\n"]

    total_successes = test_suite.total_successes()
    total_errors = test_suite.total_errors()
    total_tests = test_suite.total_tests()

    # Add a summary section
    report_lines.append("## 📊 Summary\n")
    report_lines.append(f"- **Elasticsearch Version:** {test_suite.es_version}")
    report_lines.append(f"- **Total Tests:** {total_tests}")
    report_lines.append(f"- **Total Tested Documents:** {test_suite.total_tested_documents()}")
    report_lines.append(f"- ✅ **Total Successes Documents:** {total_successes}")
    report_lines.append(f"- ❌ **Total Errors Documents:** {total_errors}\n")

    # Add details for each test
    report_lines.append("## 🧩 Detailed Results\n")

    for test_name, result in test_suite.results.items():
        status_emoji = "✅" if result.errors == 0 else "❌"
        report_lines.append(f"### {status_emoji} {test_name}")
        report_lines.append(f"- **Successes:** {result.successes}")
        report_lines.append(f"- **Errors:** {result.errors}")

        if result.error_diffs:
            report_lines.append("\n<details><summary>🧾 Error Details</summary>\n")
            for i, diff in enumerate(result.error_diffs, 1):
                report_lines.append(f"**Difference {i}:**")
                report_lines.append("```json")
                report_lines.append(json.dumps(diff.diff, indent=2, default=str))
                report_lines.append("```")

                report_lines.append(f"- **Document ID:** `{diff.doc_id}`")
                if diff.doc_es:
                    report_lines.append(f"- **Elasticsearch Document:**")
                    report_lines.append(f"```json")
                    report_lines.append(f"{json.dumps(diff.doc_es, indent=2, default=str)}")
                    report_lines.append(f"```")
                if diff.doc_ls:
                    report_lines.append(f"- **Logstash Document:**")
                    report_lines.append(f"```json")
                    report_lines.append(f"{json.dumps(diff.doc_ls, indent=2, default=str)}")
                    report_lines.append(f"```")

            report_lines.append("</details>\n")
        else:
            report_lines.append("No differences detected.\n")
    return "\n".join(report_lines)

def write_report_to_file(report: str, filename: str):
    # Write Markdown report
    output_dir = Path(os.getenv("OUTPUT_DIR", "/output"))
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / filename

    with open(report_path, "w", encoding="utf-8") as report_file:
        report_file.write(report)

    logger.info(f"📝 Markdown report written to '{report_path}'")

def generate_elasticsearch_bulk_new(docs: List[dict], pipeline_name: str):
    """Generates documents for the Elasticsearch bulk API."""
    for doc in docs:
        logger.debug(f"Processing document for bulk upload: {doc}")
        yield {
            '_index': ES_INDEX,
            '_id': doc["@metadata"]["id"],
            'pipeline': pipeline_name,
            '_source': doc
        }

@dataclass
class BaffoOptions:
    log_level: str = "error"
    pipeline_threshold: int = 1
    deal_with_error_locally: bool = True
    add_default_global_on_failure: bool = True
    fidelity: bool = True


def getDefaultBaffoOptions() -> BaffoOptions:
    return BaffoOptions()

def getIdiomaticBaffoOptions() -> BaffoOptions:
    return BaffoOptions(log_level="info",
                        pipeline_threshold=10,
                        deal_with_error_locally=False,
                        add_default_global_on_failure=True,
                        fidelity=True)


def convert_logstash_to_es_pipeline(logstash_pipeline_path: str, options: BaffoOptions) -> dict:
    """Converts a Logstash pipeline to an Elasticsearch ingest pipeline using baffo."""
    try:
        command = f"baffo transpile {logstash_pipeline_path}"
        if options.log_level:
            command += f" --log_level={options.log_level}"
        if options.pipeline_threshold:
            command += f" --pipeline_threshold={options.pipeline_threshold}"
        if options.deal_with_error_locally:
            command += f" --deal_with_error_locally={str(options.deal_with_error_locally).lower()}"
        if options.add_default_global_on_failure:
            command += f" --add_default_global_on_failure={str(options.add_default_global_on_failure).lower()}"
        if options.fidelity:
            command += f" --fidelity={str(options.fidelity).lower()}"

        result = subprocess.check_output(command, shell=True)
        s = result.decode("utf-8")
        d = json.loads(s)
        return d
    except Exception as e:
        logger.error(f"Error transpiling Logstash pipeline {logstash_pipeline_path}")
        logger.exception(e)

def getDocumentSourceById(es: elasticsearch.Elasticsearch, index: str, doc_id: str) -> Optional[Dict]:
    """Fetches a document from Elasticsearch by index and ID."""
    try:
        response = es.get(index=index, id=doc_id)
        return response["_source"]
    except elasticsearch.exceptions.NotFoundError:
        return None

# --- Configuration ---

# Configuration variables (keep as is)
ES_INDEX = 'test-elastic'
LS_INDEX = 'test-logstash'



# --- Logging Setup ---

# Corrected logger setup for ECS compliance
# The logging.StreamHandler() object should be configured and then added to the logger.
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ecs_logging.StdlibFormatter())
logger = logging.getLogger('output_uploader')
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# ------------------------------------------------
# ADD THIS SECTION TO SUPPRESS THE WARNING
# ------------------------------------------------
logger.info("Disabling InsecureRequestWarning for testing purposes...")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Elasticsearch Connection ---

# Client setup (keep as is, assuming Elasticsearch is running locally with default credentials)
es = elasticsearch.Elasticsearch(
    hosts=[os.environ.get("ES_HOST", "https://localhost:9200")],
    verify_certs=False,
    basic_auth=("elastic", "changeme")
)

es_version = es.info().get('version', {}).get('number', 'unknown')

# --- Make sure the indices do not exist ---
es.options(ignore_status=[400, 404]).indices.delete(index=ES_INDEX)
es.options(ignore_status=[400, 404]).indices.delete(index=LS_INDEX)

# Create the empty indices
# We are not interested in mapped fields but only in checking if the sources are the same
# This allows us to have also test documents that fails
es.options(ignore_status=[400, 404]).indices.create(index=ES_INDEX, mappings={"dynamic": False})
es.options(ignore_status=[400, 404]).indices.create(index=LS_INDEX, mappings={"dynamic": False})
logger.info(f"Indices '{ES_INDEX}' and '{LS_INDEX}' ensured to exist.")

test_suite = TestSuite(es_version=es_version)

for test in os.listdir("test"):
    if not os.path.isdir(os.path.join("test", test)):
        continue

    logstash_pipeline_path = os.path.join("test", test, "pipeline.conf")
    ingest_pipelines_path = os.path.join("test", test, "pipelines.json")
    docs_path = os.path.join("test", test, "docs.ndjson")

    # Convert the pipeline with baffo...
    logger.info(f"Converting Logstash pipeline in {logstash_pipeline_path}")
    try:
        result = convert_logstash_to_es_pipeline(logstash_pipeline_path, getIdiomaticBaffoOptions())
        with open(ingest_pipelines_path, "w") as f:
            json.dump(result, f, indent=2)
    except Exception as e:
        logger.error(f"Error transpiling Logstash pipeline {logstash_pipeline_path}")
        logger.exception(e)
        sys.exit(1)


    # --- Ingest Pipeline Upload ---


    pipelines = {}
    pipeline_conversion_error = False


    try:
        with open(ingest_pipelines_path, "r") as f:
            pipelines = json.load(f)

        for p in pipelines:
            # Use the modern client interface
            try:
                pipeline_def = pipelines[p]
                pipeline_name = p
                es.ingest.put_pipeline(id=pipeline_name, **pipeline_def)
                logger.info(f"Uploaded Ingest Pipeline: {pipeline_name}")
            except Exception as e:
                logger.error(f"Error uploading ingest pipeline: {pipeline_name}")
                logger.error(pipeline_def)
                logger.exception(e)
                pipeline_conversion_error = True
                sys.exit(1)

    except FileNotFoundError:
        logger.error(f"Error: The file '{ingest_pipelines_path}' was not found.")
        pipeline_conversion_error = True

    if pipeline_conversion_error:
        logger.error("There were errors uploading the ingest pipelines")
        sys.exit(1) # Commented out for non-fatal testing, uncomment in production


    # Logstash Pipeline Upload
    try:
        logger.info(f"Processing Logstash pipeline file: {logstash_pipeline_path}")
        with open(logstash_pipeline_path, "r") as content:

            pipeline_id = f"pipeline-{test}"

            default_elasticsearch_output = f"""output {{
    elasticsearch {{
        hosts => ["es01:9200"]
        index => "{LS_INDEX}"
        document_id => "%{{[@metadata][id]}}"
        user => "elastic"
        password => "changeme"
        ssl_enabled => true
        ssl_verification_mode => "none"
        # cacert => "/usr/share/logstash/config/certs/ca/ca.crt
    }}
}}
"""

            default_input = f"""input {{
    file {{
        path => ["/usr/share/logstash/{docs_path}"]
        codec => json {{
            target => ""
            ecs_compatibility => "disabled"
        }}
        # mode => "read"
        start_position => "beginning"
        sincedb_path => "/dev/null"
        # exit_after_read => true
    }}
}}
"""
            es.options(ignore_status=[400, 404]).logstash.delete_pipeline(id=pipeline_id)

            # Wait a bit to ensure deletion is processed and Logstash removes the pipeline
            sleep(5)

            # Use the correct client method for Logstash pipeline management
            es.logstash.put_pipeline(
                id=pipeline_id,
                pipeline={
                    "username": "mirko",
                    "pipeline_metadata": {},
                    "last_modified": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    "pipeline_settings": {
                    },
                    # Assuming content.read() provides the Logstash configuration string
                    "pipeline": default_input +
                        content.read() +
                        "\n" +
                        default_elasticsearch_output
                }
            )
            logger.debug(f"Uploaded Logstash pipeline: {pipeline_id}")
    except Exception as e:
        logger.error(f"Error uploading Logstash pipeline {logstash_pipeline_path}")
        logger.exception(e)
        sys.exit(1)





    # --- Data Indexing (Bulk Upload) ---

    with open(docs_path, "r") as f:
        docs = [json.loads(line) for line in f.readlines() if line.strip()]

        try:

            # The generator function handles potential JSON errors
            successes, errors = bulk(es, generate_elasticsearch_bulk_new(docs, "main-pipeline-pipeline"), raise_on_error=False)

            if errors:
                logger.error(f"Bulk indexing for {docs_path} completed with errors: {len(errors)}")
                for err in errors:
                    logger.debug(f"Bulk error: {err}")
            else:
                logger.info(f"Successfully indexed {successes} documents from {docs_path}.")

        except BulkIndexError as bie:
            # This block will not be hit if raise_on_error=False is used in bulk()
            logger.exception(bie)
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing file {docs_path}")
            logger.exception(e)


    errors = 0
    error_diffs: List[TestDiff] = []
    # Wait for indexing to complete and for Elasticsearch to refresh the indices
    logger.info("Waiting 5 seconds for Elasticsearch to refresh indices...")
    sleep(5)
    es.indices.refresh(index=ES_INDEX)
    es.indices.refresh(index=LS_INDEX)

    test_result = TestResult(0, 0, [])


    with open(docs_path, "r") as f:
        docs = [json.loads(line) for line in f.readlines() if line.strip()]

        for doc in docs:

            if len(doc) == 0:
                continue

            doc_id = doc["@metadata"]["id"]

            # Get documents from both indices
            logger.info(f"Comparing documents with ID: {doc_id}...")

            es_example = getDocumentSourceById(es, ES_INDEX, doc_id)
            ls_example = getDocumentSourceById(es, LS_INDEX, doc_id)

            notFound = False

            messageToAppend = ""

            if es_example is None:
                notFound = True
                messageToAppend += f"Document with ID {doc_id} not found in index '{ES_INDEX}'."
            if ls_example is None:
                notFound = True
                messageToAppend += f"Document with ID {doc_id} not found in index '{LS_INDEX}'."

            if notFound:
                logger.error(messageToAppend)
                test_result.errors += 1
                test_result.error_diffs.append(TestDiff(DeepDiff(f"{messageToAppend}", {}), doc_id=doc_id, doc_es=es_example, doc_ls=ls_example, doc_original=doc))
                continue


            # Compare sources
            ddiff = DeepDiff(
                es_example,
                ls_example,
                ignore_order=True,
                exclude_paths=["root['@version']", "root['@timestamp']", "root['host']", "root['log']", "root['_TRANSPILER']", "root['ecs']", "root['@metadata']"]
            )

            if bool(ddiff):
                test_result.errors += 1
                test_result.error_diffs.append(TestDiff(ddiff, doc_id=doc_id, doc_es=es_example, doc_ls=ls_example, doc_original=doc))
            else:
                test_result.successes += 1
                logger.debug(f"Documents match for ID: {doc_id}")


    test_suite.add_result(test, test_result)

# --- Final Output and Assertion ---




logger.info(f"ℹ️ There is/are {test_suite.total_errors()} error(s) in the comparison along with {test_suite.total_successes()} success(es).\n")




# Assert and final print statement
if test_suite.total_errors() == 0:
    logger.info("Test Successful 🎉")

else:
    for t in test_suite.results:
        result = test_suite.results[t]
        if result.errors == 0:
            logger.info(f"Test '{t}' Successful 🎉: {result.successes} success(es) found.")
        else:
            logger.error(f"Test '{t}' Failed ❌: {result.errors} difference(s) found")
            for ed in result.error_diffs:
                logger.error(ed)



markdown_report = generate_markdown_report(test_suite)

write_report_to_file(markdown_report, "baffo-testsuite-report.md")