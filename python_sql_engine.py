"""Python SQL engine

This script contains four functions which correspond respectively to WHERE, INNER JOIN, GROUP BY,ORDER BY in SQL
All the four functions are abstract methods which are generalized on all sql queries
"""
import csv
from collections import defaultdict
from collections import namedtuple
from itertools import groupby
from operator import attrgetter


def read_with_where_filter(path: str, name: str, filters: dict = {}) -> list:
    """
    Read a csv file as a list of namedtuples and apply where filter
    :param path (string): The relative path of the csv file to read
    :param name (string): The name of the namedtuple which represent the entity of the data in table
    :param filters (dict): Filters of key-value pair({column: required value}) that should be applied to the table
    :return (list): read data in the format of list of namedtuples
    """
    with open(path, "r") as csv_file:
        reader = csv.reader(csv_file)
        entity = namedtuple(name, next(reader), rename=True)
        results = []
        for row in reader:
            row = entity(*row)
            if all(getattr(row, key) == value for key, value in filters.items()):
                results.append(row)
    return results


def inner_hash_join(left: list, right: list, join_key: str) -> list:
    """
    Inner join two tables: left and right using hash join. Hashing is applied to the right table. Put bigger table on the left and the smaller one on the right.
    This funcion also rename the overlapped join_key in the result set, with '_left' and '_right' suffix
    :param left (list): left table to join
    :param right (list): right table to join (hash)
    :param join_key (string): common key to join on in the two tables
    :return (list): joined data in the format of list of namedtuples
    """
    results = []
    right_index = {
        key: list(value)
        for key, value in groupby(sorted(right, key=attrgetter(join_key)), lambda x: getattr(x, join_key),)
    }
    left_cols_key_prefix = [col + "_left" if col == join_key else col for col in left[0]._fields]
    right_cols_key_perfix = [col + "_right" if col == join_key else col for col in right[0]._fields]
    results_col = left_cols_key_prefix + right_cols_key_perfix
    results_entity = namedtuple("join_results", results_col)
    for row_left in left:
        right_matches = right_index.get(getattr(row_left, join_key))
        if right_matches:
            results.extend([results_entity(*row_left, *row_right) for row_right in right_matches])
    return results


def group_by(data: list, group_key: str, aggregation_cols: list[str]) -> dict:
    """
    :param data (list): The base dataset where we need to apply group by
    :param group_key (string): The column to group by
    :param aggregation_cols (list): Columns where we want to apply aggregation.
    :return (dict): Grouped-by data in the format of dictionary with group-by column as key and all the records of aggregation columns
    """
    grouped_data = defaultdict(list)
    for row in data:
        grouped_data[getattr(row, group_key)].append([getattr(row, col) for col in aggregation_cols])
    return grouped_data


def order_by(data: list, orders: list[tuple]) -> list:
    """
    ORDER BY data according the specifications passed through the 'orders' argument
    :param data: The base dataset where we need to apply order by
    :param orders: The ordering specification. Each element is a tuple of col_index and ordering direction
    :return: Ordered dataset according to specifications
    """
    for index, order in orders[::-1]:
        data.sort(key=lambda x: x[index], reverse=(order == "DESC"))
    return data


if __name__ == "__main__":
    # Read data from 2 csv files and apply filter in where clause
    billed_orders = read_with_where_filter("csv/orders.csv", "order", {"is_billed": "True"})
    member_users = read_with_where_filter("csv/users.csv", "user", {"is_member": "True"})

    # inner join to keep only the orders of memebers, indexing user_id for linear complexity
    member_user_orders = inner_hash_join(billed_orders, member_users, "user_id")

    # group by data using a default dictionary. The key is country code, the values are corresponding list of values
    country_orders = group_by(member_user_orders, "country", ["amount", "is_shipped"])

    # apply aggregation functions (sum & count distinct) on grouped data
    aggregated_data = []
    for key, values in country_orders.items():
        aggregated_data.append(
            [key, sum([float(value[0]) for value in values]), len([value[1] == True for value in values]),]
        )

    # order by results using sorted function
    results = order_by(aggregated_data, [(1, "DESC"), (2, "ASC")])

    # print results to stdout
    print("country, sum_amount, nb_of_shipped_orders")
    for result in results:
        print(*result, sep=", ")
