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


def concat_col_names_add_suffix(left_cols, right_cols, left_join_key, right_join_key):
    """
    This funcion also rename the overlapped join_key in the result set, with '_left' and '_right' suffix
    """
    left_cols_key_suffix = [col + "_left" if col == left_join_key else col for col in left_cols]
    right_cols_key_suffix = [col + "_right" if col == right_join_key else col for col in right_cols]
    return left_cols_key_suffix + right_cols_key_suffix

def inner_hash_join(left: list, right: list, left_join_key: str, right_join_key: str) -> list:
    """
    Inner join two tables: left and right using hash join. Hashing is applied to the right table. Put bigger table on the left and the smaller one on the right.
    This funcion also rename the overlapped join_key in the result set, with '_left' and '_right' suffix
    :param left (list): left table to join
    :param right (list): right table to join (hash)
    :param left_join_key (string): joined key of the left table
    :param right_join_key (string): joined key of the right table
    :return (list): joined data in the format of list of namedtuples
    """
    results = []
    right_index = {
        key: list(value)
        for key, value in groupby(right, lambda x: getattr(x, right_join_key))
    }
    results_cols = concat_col_names_add_suffix(left[0]._fields, right[0]._fields, left_join_key, right_join_key)
    results_entity = namedtuple("join_results", results_cols)
    for row_left in left:
        right_matches = right_index.get(getattr(row_left, left_join_key))
        if right_matches:
            results.extend([results_entity(*row_left, *row_right) for row_right in right_matches])
    return results


def merge_join(left: list, right: list, left_join_key: str, right_join_key: str) -> list:
    """
    Merge join two tables: Sort the two tables by their keys first and loop the sorted table together
    :param left (list): left table to join
    :param right (list): right table to join (hash)
    :param left_join_key (string): joined key of the left table
    :param right_join_key (string): joined key of the right table
    :return (list): joined data in the format of list of namedtuples
    """
    results = []
    i, j = 0, 0
    left = sorted(left, key=lambda x: getattr(x, left_join_key))
    right = sorted(right, key=lambda x: getattr(x, right_join_key))
    results_cols = concat_col_names_add_suffix(left[0]._fields, right[0]._fields, left_join_key, right_join_key)
    results_entity = namedtuple("join_results", results_cols)
    while i < len(left) and j < len(right):
        left_key_value = getattr(left[i], left_join_key)
        right_key_value = getattr(right[j], right_join_key)
        if left_key_value < right_key_value:
            i += 1
        elif left_key_value > right_key_value:
            j += 1
        else:
            temp_i, temp_j = i, j
            while temp_i < len(left) and getattr(left[temp_i], left_join_key) == left_key_value:
                temp_j = j
                while temp_j < len(right) and getattr(right[temp_j], right_join_key) == right_key_value:
                    results.append(results_entity(*left[temp_i], *right[temp_j]))
                    temp_j += 1
                temp_i += 1
            i, j = temp_i, temp_j
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

    # inner join (either hash join or merge join) to keep only the orders of memebers, indexing user_id for linear complexity
    member_user_orders = inner_hash_join(billed_orders, member_users, "user_id", "user_id")
    # member_user_orders = merge_join(billed_orders, member_users, "user_id", "user_id")

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
