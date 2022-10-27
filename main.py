from datetime import datetime, timedelta
from multiprocessing import Pool
from os import getenv
import logging
import google.cloud.storage
import google.cloud.logging
from google.cloud.exceptions import NotFound

from get_stats import get_dgg_stats, process_dgg_stats
from write_stats import write_dgg_stats, define_tables

cloud_sync = True

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if cloud_sync:
    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging(log_level=logging.DEBUG)


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def main(start_date=datetime.today() - timedelta(days=1), end_date=None):
    logger.info(f"Starting get-dgg-stats")
    if not end_date:
        end_date = start_date
    next_day = end_date + timedelta(days=1)
    db_name = "dgg_stats.db"
    db_name = getenv("DGG_STATS_DB")
    logger.debug(f"Downloading {db_name}...")
    storage_client = google.cloud.storage.Client()
    bucket = storage_client.bucket("tenadev")
    db_blob = bucket.blob(f"website/{db_name}")
    db_blob.download_to_filename(db_name)

    for day in daterange(start_date, next_day):
        log_filename = f"{day.strftime('%Y-%m-%d')}.txt"
        if cloud_sync:
            log_blob = bucket.blob(f"dgg-logs/{log_filename}")
            try:
                logs = log_blob.download_as_string().split("\n")
            except NotFound:
                logger.warning(f"Couldn't download {log_filename}")
                continue
            logger.debug(f"Downloaded {log_filename}")
        else:
            with open(log_filename, "r") as log_file:
                logs = log_file.read().split("\n")
        user_index = define_tables(return_users=True)
        logs_with_ui = [(log, user_index) for log in logs]
        with Pool() as pool:
            logger.debug("Getting stats...")
            stats = pool.starmap(get_dgg_stats, logs_with_ui)
        write_dgg_stats(process_dgg_stats(stats), day)

    logger.debug("Uploading db...")
    db_blob.upload_from_filename(db_name)

    logger.info(f"Finished get-dgg-stats")


if __name__ == "__main__":
    main()
