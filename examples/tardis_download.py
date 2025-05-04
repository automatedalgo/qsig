
from qsig.data.tardis.tardis_downloader import TardisDownloader
import qsig


def main():
    qsig.init()

    # Put your Tardis API key here.  If you don't have a Tardis subscription,
    # you can leave this empty, and download free data; Tardis does allow free
    # access to data for the first day of each month.
    api_key = ""

    # Describe the data file to download
    dataset = "trades"
    exchange = "binance-futures"  # or: "binance"
    symbol = "BTCUSDT"
    year=2025
    month=5
    day=1

    # create the Tardis downloader
    tardis = TardisDownloader(api_key=api_key)

    # execute the download request
    tardis.fetch_csv_file(year=year,
                          month=month,
                          day=day,
                          dataset=dataset,
                          exchange=exchange,
                          symbol=symbol)

if __name__ == "__main__":
    main()
