import argparse
import logging
import os
import time
import psycopg2
from dotenv import load_dotenv

from pipeline.extract import extract_and_augment
from pipeline.quality import run_quality_gate
from pipeline.transform import clean_and_transform_record
from pipeline.load import load_to_silver, populate_warehouse

load_dotenv()

SRC_DB_CONFIG = dict(
    host=os.getenv("SRC_DB_HOST"), port=os.getenv("SRC_DB_PORT"),
    dbname=os.getenv("SRC_DB_NAME"), user=os.getenv("SRC_DB_USER"), password=os.getenv("SRC_DB_PASSWORD")
)
DEST_DB_CONFIG = dict(
    host=os.getenv("DEST_DB_HOST"), port=os.getenv("DEST_DB_PORT"),
    dbname=os.getenv("DEST_DB_NAME"), user=os.getenv("DEST_DB_USER"), password=os.getenv("DEST_DB_PASSWORD")
)


def parse_args():
    parser = argparse.ArgumentParser(description="METABRIC ETL pipeline")
    parser.add_argument("--log-file", type=str, default="pipeline.log")
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


def main():
    args = parse_args()
    setup_logging(args.log_file)

    src_conn = psycopg2.connect(**SRC_DB_CONFIG)
    dst_conn = psycopg2.connect(**DEST_DB_CONFIG)
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
        populate_warehouse(src_conn, dst_conn)
        logger.info(f"Warehouse population completed in {time.time() - time0:.2f}s")

        logger.info("Pipeline run complete")
    finally:
        src_conn.close()
        dst_conn.close()
        logger.info("Database handles closed successfully.")


if __name__ == "__main__":
    main()