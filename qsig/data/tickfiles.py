from dataclasses import dataclass
from pathlib import Path
import datetime as dt
import os
import logging

import qsig


@dataclass(frozen=True)
class TickFileURI:
    filename: str
    collection: str
    venue: str
    dataset: str
    date: dt.date
    symbol: str
    tick_home: Path = qsig.settings.tick_data_home()

    @property
    def folder(self) -> Path:
        parts = [self.tick_home.as_posix(),
                 self.collection,
                 self.venue,
                 self.dataset,
                 f"{self.date.year:04d}",
                 f"{self.date.month:02d}",
                 f"{self.date.day:02d}"]
        return Path("/".join(parts))

    @property
    def path(self) -> Path:
        parts = [self.tick_home.as_posix(),
                 self.collection,
                 self.venue,
                 self.dataset,
                 f"{self.date.year:04d}",
                 f"{self.date.month:02d}",
                 f"{self.date.day:02d}",
                 self.filename]
        return Path("/".join(parts))

    def replace(self, dataset=None, filename=None, collection=None):
        return TickFileURI(
            filename=filename or self.filename,
            collection=collection or self.collection,
            venue=self.venue,
            dataset=dataset or self.dataset,
            date=self.date,
            symbol=self.symbol,
            tick_home=self.tick_home)


def _path_is_tickdata_compliant(path: Path) -> bool:
    """Determine if a directory path conforms to a valid tick data path. A value
    tick data path should end with three subdirectories like: yyyy/mm/dd
    """
    if len(path.parts) < 4:
        return False
    return True


def _build_tick_file_uri(base_dir, pair):
    path = Path(pair[0].removeprefix(f"{base_dir}/"))
    if not _path_is_tickdata_compliant(path):
        return None
    collection = path.parts[0]   # eg 'tardis', 'bin1'
    venue = path.parts[1]  # example: 'binance'
    dataset = path.parts[2]
    year = int(path.parts[3])
    month = int(path.parts[4])
    day = int(path.parts[5])
    symbol = pair[1].split('.')[0]
    uri = TickFileURI(
        filename=pair[1],
        collection=collection,
        venue=venue,
        dataset=dataset,
        date=dt.date(year=year, month=month, day=day),
        symbol=symbol,
        tick_home=base_dir
    )
    return uri


def scan_tick_files():
    tick_home = qsig.settings.tick_data_home()
    tick_file_registry = dict()
    for root, dirs, files in os.walk(tick_home):
        if root == tick_home:
            continue  # no tick files in base directory
        for fn in files:
            try:
                uri = _build_tick_file_uri(tick_home, (root, fn))
                if uri is None:
                    logging.warning(f"skipping {root}/{fn}")
                else:
                    tick_file_registry[uri.path] = uri
            except Exception as e:
                logging.warning(f"skipping {root}/{fn} : {e}")

    return list(tick_file_registry.values())
