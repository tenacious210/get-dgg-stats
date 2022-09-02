from get_stats import get_dgg_stats, process_dgg_stats
from write_stats import write_dgg_stats
from datetime import datetime, timedelta
from multiprocessing import Pool
import requests

from google.cloud import storage


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def main(start_date=datetime.today() - timedelta(days=1), end_date=None):
    print(f"Starting at {datetime.now()}")
    if not end_date:
        end_date = start_date
    next_day = end_date + timedelta(days=1)

    print("Downloading dgg_stats.db...")
    storage_client = storage.Client()
    bucket = storage_client.bucket("tenadev")
    blob = bucket.blob("dgg_stats.db")
    blob.download_to_filename("dgg_stats.db")

    for day in daterange(start_date, next_day):
        date_str = day.strftime("%Y %m %B %d").split()
        rustle_url = (
            "https://dgg.overrustlelogs.net/Destinygg%20chatlog/"
            f"{date_str[2]}%20{date_str[0]}/{date_str[0]}-{date_str[1]}-{date_str[3]}.txt"
        )
        print(f"Requesting logs for {day.strftime('%Y-%m-%d')}...")
        logs = requests.get(rustle_url).text.split("\n")
        with Pool() as pool:
            print("Getting stats...")
            stats = pool.map(get_dgg_stats, logs)
        write_dgg_stats(process_dgg_stats(stats), day)

    print("Uploading db...")
    blob.upload_from_filename("dgg_stats.db")

    print(f"Finished at {datetime.now()}")


if __name__ == "__main__":
    main()
