import csv
from math import ceil
from typing import NamedTuple, List, Generator
from io import IOBase


class CsvRowWriter:
    """
    Provides a destination for the CSV writer resulting in a list of strings where each string is a line of CSV
    """

    rows: List[str]

    def __init__(self):
        self.rows = []

    def write(self, row):
        self.rows.append(row)


CsvParsed = NamedTuple(
    "CsvParsed",
    [
        ("columns", List[str]),
        ("rows", List[str]),
    ],
)


def parse(file: IOBase) -> CsvParsed:
    """
    Parses a CSV and returns columns as a list and a list of rows encoded as strings
    """
    reader = csv.reader(file, delimiter=",")
    headers = reader.__next__()
    data = CsvRowWriter()
    writer = csv.writer(data, lineterminator="")
    for r in reader:
        writer.writerow(r)
    return CsvParsed(columns=headers, rows=data.rows)


def batch(total, batch_size) -> Generator:
    """
    Used to batch large files in to smaller chunks to import
    """
    for i in range(ceil(total / batch_size)):
        if (i + 1) * batch_size >= total:
            yield i * batch_size, total
            break
        yield i * batch_size, (i + 1) * batch_size
