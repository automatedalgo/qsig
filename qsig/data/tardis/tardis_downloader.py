import datetime as dt
import os
import requests
import logging

from qsig.data.tickfiles import TickFileURI
import qsig

# Download Tardis CSV datasets and save them locally into tickdata home.
class TardisDownloader:

    BASE_URL = "https://datasets.tardis.dev/v1"

    def __init__(self,
                 *,
                 api_key: str,
                 base_tick_data_dir = None,
                 base_url: str = None):
        if base_tick_data_dir is None:
            base_tick_data_dir = qsig.settings.tick_data_home()
        self.base_tick_data_dir = base_tick_data_dir
        self.api_key = api_key
        self.base_url = base_url or TardisDownloader.BASE_URL


    def fetch_csv_file(self,
                       *,
                       year: int,
                       month: int,
                       day: int,
                       dataset: str,
                       exchange: str,
                       symbol: str):
        url = f"{self.base_url}/{exchange}/{dataset}/{year:04d}/{month:02d}/{day:02d}/{symbol}.csv.gz"

        local_uri = TickFileURI(
            filename=f"{symbol}.csv.gz",
            collection="tardis",
            venue=exchange,
            dataset=dataset,
            date=dt.date(year, month, day),
            symbol=symbol,
            tick_home=self.base_tick_data_dir)

        if os.path.isfile(local_uri.path):
            logging.info(f"already exists: {local_uri.path}")
        else:
            os.makedirs(local_uri.folder, exist_ok=True)
            headers = {"Authorization": f"Bearer {self.api_key}"}
            logging.info(f"fetching: {url}")
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                logging.error("error: {}".format(response.text))
            response.raise_for_status()
            logging.info(f"writing: {local_uri.path}")

            with open(local_uri.path, "wb") as f:
                f.write(response.content)
        return url




    def fetch_csv_files_for_dates(self,
                                  *,
                                  date_from: dt.date,
                                  date_upto: dt.date,
                                  dataset: str,
                                  exchange: str,
                                  symbol: str):
        assert date_upto > date_from
        dates = [(date_from+dt.timedelta(days=x)) for x in range((date_upto - date_from).days)]
        for date in dates:
            self.fetch_csv_file(year=date.year, month=date.month,
                                day=date.day, dataset=dataset,
                                exchange=exchange, symbol=symbol)
