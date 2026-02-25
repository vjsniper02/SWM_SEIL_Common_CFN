from io import StringIO

import pytest

from app.csv_parser import batch, parse

mock_csv1 = """a,b,c,d,e
x,y,z,1,2
foo,bar,baz,"1,2","3,4"
"""


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ((50, 100), [(0, 50)]),
        ((100, 100), [(0, 100)]),
        ((299, 100), [(0, 100), (100, 200), (200, 299)]),
        ((300, 100), [(0, 100), (100, 200), (200, 300)]),
        ((301, 100), [(0, 100), (100, 200), (200, 300), (300, 301)]),
    ],
)
def test_batch(test_input, expected):
    assert list(batch(*test_input)) == expected


def test_parse():
    imported = parse(StringIO(mock_csv1))
    assert len(imported.columns) == 5
    assert len(imported.rows) == 2
    assert imported.rows[1] == 'foo,bar,baz,"1,2","3,4"'
