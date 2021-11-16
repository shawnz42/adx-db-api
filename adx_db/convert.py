
from collections import namedtuple


def convert_rows(cols, rows):
    Row = namedtuple('Row', cols, rename=True)

    results = []
    for row in rows:
        results.append(Row(*row))
        # results.append(tuple(row))

    return results


if __name__ == '__main__':
    cols_ = ['a', 'b']
    rows_ = [[1, 2],
             [3, 4]]

    print(convert_rows(cols_, rows_))
