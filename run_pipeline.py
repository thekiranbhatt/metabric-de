import argparse
import logging
import os
import time
import psycopg2
from dotenv import load_dotenv

from pipeline.extract import extract_and_augment
from pipeline.quality import run_quality_gate
from pipeline.transform import clean_and_transform_record
from pipeline.load import load_to_silver, populate_warehouse, populate_warehouse_v2

load_dotenv()

def _database_config(prefix: str) -> dict[str, str | None]:
    return {
        "host": os.getenv(f"{prefix}_DB_HOST"),
        "port": os.getenv(f"{prefix}_DB_PORT"),
        "dbname": os.getenv(f"{prefix}_DB_NAME"),
        "user": os.getenv(f"{prefix}_DB_USER"),
        "password": os.getenv(f"{prefix}_DB_PASSWORD"),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="METABRIC ETL pipeline")
    parser.add_argument("--log-file", type=str, default="pipeline.log")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Warehouse sync mode; incremental requires a previous full warehouse load",
    )
    return parser.parse_args()


def setup_logging(log_filename):
    log_format = "%(asctime)s  %(levelname)s [%(filename)s:%(lineno)d] %(message)s"
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=logging.INFO, format=log_format, force=True,
        handlers=[logging.FileHandler(log_filename, mode="w"), logging.StreamHandler()]
    )


logger = logging.getLogger(__name__)


def run_pipeline(
    mode: str = "full",
    src_config: dict | None = None,
    dest_config: dict | None = None,
):
    src_conn = psycopg2.connect(**(src_config or _database_config("SRC")))
    dst_conn = psycopg2.connect(**(dest_config or _database_config("DEST")))
    try:
        time0 = time.time()
        raw_records = extract_and_augment(src_conn)
        logger.info(f"Extraction + augmentation completed in {time.time() - time0:.2f}s")

        time0 = time.time()
        transformed = [clean_and_transform_record(r) for r in raw_records]
        logger.info(f"Transformation completed in {time.time() - time0:.2f}s")

        time0 = time.time()
        clean, rejected = run_quality_gate(transformed)
        logger.info(f"Quality gate completed in {time.time() - time0:.2f}s")
        if rejected:
            logger.warning(f"{len(rejected)} rows rejected — see log above for reasons")

        time0 = time.time()
        load_to_silver(src_conn, clean)
        logger.info(f"Silver load completed in {time.time() - time0:.2f}s")

        time0 = time.time()
        populate_warehouse(src_conn, dst_conn, mode=mode)
        logger.info(f"Warehouse population completed in {time.time() - time0:.2f}s")

        time0 = time.time()
        populate_warehouse_v2(src_conn, dst_conn, mode=mode)
        logger.info(f"V2 warehouse population completed in {time.time() - time0:.2f}s")

        logger.info("Pipeline run complete")
    finally:
        src_conn.close()
        dst_conn.close()
        logger.info("Database handles closed successfully.")


def main():
    args = parse_args()
    setup_logging(args.log_file)
    run_pipeline(mode=args.mode)


if __name__ == "__main__":
    main()
