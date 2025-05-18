# What is QSig?

QSig is a free and open source Python framework for quant signal & strategy
research.

It provides tools & conventions to solve typical quant research infrastructure
challenges, such as downloading raw data from vendors, storing that data on
disk, transforming that data into research datasets, evaluating signals and
plotting results.

QSig is designed to be lightweight and easy to use. Quant research process is
challenging enough by itself, so a goal should be to avoid unnecessary complexity
in the tools that support it.  QSig also promotes a research workflow that
separates the concerns of data download, transformation and signal
research. This structured approach helps keep the process manageable. Quant
research is hard enough - there’s no need to make it harder with overly complex
infrastructure

This current introduction looks at pulling raw trades data from Tardis, using
them to build trade bins, and exporting the final aggregated data to later
signal research scripts.


# Tardis Data

Tardis is a data vendor that provides tick-by-tick order book and trade data for
leading cryptocurrency exchanges. It's an excellent starting point for
quantitative research, offering quick and easy access to historical data. In
this guide, we’ll walk through the steps and conventions for downloading trade
data from Tardis, binning it, and storing it in a QSig data repository.

This guide follows the example found at `examples/qsig_quick_intro.org`

# Fetching Tardis datasets

QSig provides the `TardisDownloader` class to download Tardis raw files.

    tardis = TardisDownloader(api_key)

The constructor requires an `api_key`. Tardis gives you a private key when you
purchase either a data subscription or a one-off download.  If you don’t have a
key, you can still run the code to download free data - Tardis provides datasets
for the first day of each month at no cost.

Next we define the data we wish to download - this includes the dataset, the
symbol and the date range:

    # date range
    dt_from = dt.date(2025, 2, 1)
    dt_upto = dt_from + dt.timedelta(months=1) # or days=1 for free data

    # trades dataset
    dataset = "trades"

    # instrument is BTCUSDT perp future
    tardis_exchange = "binance-futures"
    tardis_symbol = "BTCUSDT"

Tardis data is organised into daily files (this is also the convention adopted
by QSig), so downloading the full data requires iteration over the desired date
range.

    for date in date_range(dt_from, dt_upto):
        location = tardis.fetch_csv_file(year=date.year,
                                         month=date.month,
                                         day=date.day,
                                         dataset=dataset,
                                         exchange=tardis_exchange,
                                         symbol=tardis_symbol)

QSig will download the requested trade files and save them directly to disk (if
the data has already been downloaded, it is not fetched a second time).  The
saved location is returned, if you wish to read the file immediately.  However
you typically don't need to use `location` because other QSig functions will know
where to look to find the data.

This is because QSig follows a "convention over configuration" approach, which
includes automatically determining the download location. Organising data in a
clear and consistent structure is a key part of building a productive quant
research environment, making data easy to locate and manage.

By default, vendor files are saved under `~/mdhome/tickdata` - this is the root
market-data directory (this can be changed by defining the environment variable
`QSIG_TICKDATA_DIR`). Within this directory, files are organised using the
following hierarchy: vendor, venue, dataset, year, month, day, instrument.  For
this current example, the saved location will be:

    ~/mdhome/tickdata/tardis/binance-futures/trades/2025/02/01/BTCUSDT.csv.gz

After the Python loop completes, the tick data folder will contain a collection
of trades files, one for each date of the requested date range.


# Trade Bins

For quantitative research, it’s typically more convenient to work with binned
data instead of individual trades.  Raw trades occur at irregular time points,
and can be high volume, making it memory & CPU resource-intensive to process. In
contrast, binned data is regular and smaller, which is more efficient to work
with. QSig provides tools to convert trades into time bins, which we’ll explore
next.

To build trade bins from Tardis raw trades, QSig provides the function
`create_trade_bins`. This needs to told which data to process (defined by the
instrument and date range), and the bin duration.

    bin_rule = "30s"  # 30 seconds
    instrument = Instrument("BTC", "USDT", ExchCode.BINANCE_FUTURES)
    create_trade_bins([instrument], dt_from, dt_upto, bin_rule)

This function will locate the trade files downloaded earlier, generate the bins,
and write them back to disk.  A single binned data file is created for each date
in the requested date range.  You can obtain the location of the saved files by
asking QSig for the resource identifier for one of the dates:

    uri = build_trade_bin_uri(instrument, dt_from, bin_rule)
    print(f"trade bin location: {uri.path}")

This will reveal the location of trade bin file, saved in parquet format.  You
can now read this directly:

    df = pd.read_parquet(uri.path)
    df.head()

                             open      high       low     close           mean        std  count  volume  buy_volume  sell_volume           vwap
    2025-02-01 00:00:30  102379.8  102379.8  102314.3  102314.3  102338.449304  19.342465   1509  55.991      23.819       32.172  102342.633291
    2025-02-01 00:01:00  102314.3  102314.4  102274.4  102274.5  102291.666936  11.765827   1485  43.181       3.986       39.195  102290.268354
    2025-02-01 00:01:30  102274.5  102313.7  102274.4  102275.2  102296.072381  12.828276   1155  43.215      19.006       24.209  102299.269138
    2025-02-01 00:02:00  102275.3  102295.4  102275.2  102291.4  102287.440025   7.746848    797  20.755       7.924       12.831  102284.254960
    2025-02-01 00:02:30  102291.4  102318.5  102277.3  102282.1  102298.146309  12.893242   1287  28.830      10.056       18.774  102298.865689


# Aggregation

Now that we have generated the 30 second trade bins, one file for each date, the
final data preparation step is to aggregate the files into a single dataframe.
A full-history dataframe is the starting place from where to begin actual quant
research.

Aggregation is just the simple process of loading and concatenating the
individual dataframes.

    dataframes = []

    for date in date_range(dt_from, dt_upto):
        uri = build_trade_bin_uri(instrument, date, bin_rule)
        dataframes.append(pd.read_parquet(uri.path))

    data = pd.concat(dataframes, axis=0)

At this point we can perform a quick plot to view the full history of the price
action. Although Python has several excellent Python plotting tools to choose
from (such as `matplotlib`), QSig does include a quick plotting utility to open up
a plot in your web browser:

    qsig.quick_plot(data, columns=["close"])


# DataRepo storage

We could now continue into research code, but instead we demonstrate another
feature of QSig, which is the data repository, or `DataRepo`. This is a
lightweight dataframe database, based on the design of *arcticdb*. It provides a
fast and simple way to store and retrieve dataframes. Within a quant research
workflow, `DataRepo` allows various scripts - such as those for data processing,
signal generation, blending, and analysis - to communicate by reading from and
writing to a shared data repository. This facilitates a clean separation of
concerns across different stages of research and encourages the normalisation of
dataframes.

The following code shows how to create a data repo and write the full history
trade bins.  The dataframe is stored at a named *key* within a named *library*.

    repo = qsig.DataRepo(f"/home/{getpass.getuser()}/DATAREPO")
    lib = repo.get_library("tardis")
    lib.write(key=f"trade_bins@{bin_rule}", data=data)

To complete this example, in a later in a signal script, we could fetch this
dataframe using the following code:

    bin_rule = "30s"
    repo = qsig.DataRepo(f"/home/{getpass.getuser()}/DATAREPO")
    lib = repo.get_library("tardis")
    data = lib.read(key=f"trade_bins@{bin_rule}")

Note that because we have embedded the bin rule into the data repo key, we can
imagine creating trade bins for different bin rules, and they would all sit
alongside each other inside the "tardis" library.


# Conclusion

This tutorial have provided a brief look at QSig, an open-source framework for
quantitative research. We have seen how it can be used to download trade data
from Tardis, convert it into time bins, and share that data with other research
scripts using a central data repository.

QSig aims to simplify the research process. Quant research doesn't need to rely
on complex, hard-to-follow code. By encouraging a clear and modular workflow -
where smaller components interact through normalised dataframes stored in a
shared repository - QSig helps researchers stay focused on market insight rather
than infrastructure.
