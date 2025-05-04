import datetime as dt

from qsig.data.tardis.tardis_binner import create_trade_bins
from qsig.model.instrument import Instrument, ExchCode
import qsig


def main():
    qsig.init()

    bin_rule = "5s"
    instruments = [Instrument("BTC", "USDT", ExchCode.BINANCE_FUTURES)]

    dt_from = dt.date(2025,5,1)
    dt_upto = dt.date(2025,5,2)
    create_trade_bins(instruments, dt_from, dt_upto, bin_rule)

    # # NEXT: create a single dataframe?
    # dfs = []

    # inst = Instrument("SOL", "USDT", ExchCode.BINANCE_FUTURES)
    # symbol = f"{inst.base}{inst.quote}"
    # for date in date_range(dt_from, dt_upto):
    #     bin_uri = TickFileURI(
    #         filename=f"{symbol}.parquet",
    #         collection="qsig",
    #         venue=Exchange_Map[inst.exch].slug,
    #         dataset=f"trades@{bin_rule}",
    #         date=date,
    #         symbol=symbol)
    #     dfs.append(pd.read_parquet(bin_uri.path))


    # # TOOD: quick way to see if sorted?
    # df = pd.concat(dfs)
    # assert bool((df.sort_index().index == df.index).all())

    # # repository to store the final data
    # repo = qsig.DataRepo("/var/tmp/MY_DATA_REPO")
    # lib = repo.get_library(name=f"research")
    # lib.write(f"{symbol}-trades@{bin_rule}", df)



if __name__ == "__main__":
    main()
