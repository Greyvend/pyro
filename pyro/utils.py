import string
from json import JSONEncoder
import datetime
from random import choice

from iso8601 import parse_date, ParseError
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.sql.visitors import VisitableType


def all_attributes(relations):
    """
    All attributes found in given relations

    :type relations: list
    :param relations: relations to scan
    :return: dict, containing all common attributes
    """
    attribute_dicts = (r['attributes'] for r in relations)
    # merge attribute dictionaries
    return {k: v for attrs in attribute_dicts for k, v in attrs.items()}


def containing_relation(relations, attribute_name):
    """
    Find relation among provided that contains specified attribute.

    :param relations: list of relations
    :param attribute_name: name of the attribute to search
    """
    for r in relations:
        if attribute_name in r['attributes']:
            return r
    raise ValueError('Attribute was not found in the relations specified')


def project(d, keys):
    """
    Return subdict of given dictionary containing only specified keys.
    :param d: dictionary
    :param keys: iterable with key names
    :return: dictionary with specified keys and values from initial dictionary
    """
    return {k: d[k] for k in keys}


def common_keys(d1, d2):
    """
    Return set of keys shared between two dictionaries
    :param d1: dictionary
    :param d2: dictionary
    :return: set of common key names
    """
    return set(d1) & set(d2)


def min_dict(d1, d2):
    """
    Based on two input dictionaries build third one with all the values set to
    minimum of `d1`, `d2` across the common keys

    :param d1: first input dict
    :param d2: second input dict
    """
    result = {}
    for key in common_keys(d1, d2):
        result[key] = min(d1[key], d2[key])
    return result


def all_equal(iterator):
    """
    Check that all values of iterable (interfaced by iterator) are equal

    :param iterator: any iterator, might be empty
    :return: True if all values are equal, False otherwise
    """
    try:
        iterator = iter(iterator)
        first = next(iterator)
        return all(first == rest for rest in iterator)
    except StopIteration:
        return True


def relation_name(ext_attribute_name):
    """
    Extract relation name from extended attribute name

    :param ext_attribute_name: string in a form 'Relation_name.Attribute_name'
    :return: string representing relation name ('Relation_name')
    """
    return ext_attribute_name.split('.')[0]


def attribute_name(ext_attribute_name):
    """
    Extract attribute from extended attribute name

    :param ext_attribute_name: string in a form 'Relation_name.Attribute_name'
    :return: string representing attribute ('Attribute_name')
    """
    return ext_attribute_name.split('.')[1]


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def assemble_list(l, key=lambda x: x):
    """
    Create a list of elements from list `l` of lists of elements, removing
    duplicates based on `key` function.

    :param l: iterable of other iterables containing elements to assemble
    :param key: function to apply in order to compare elements
    """
    res = []
    for sublist in l:
        for elem in sublist:
            if key(elem) not in map(key, res):
                res.append(elem)
    return res


def xstr(obj):
    """
    Convert any Python object that supports `str` descriptor, while taking care
    of tuples, stripping brackets and removing None elements if any. Simple
    NoneType also is converted to empty string instead of string `None`.

    :param obj: object to convert to string
    """
    if type(obj) is tuple:
        none_replaced = (elem for elem in obj if elem is not None)
        return ', '.join(str(elem) for elem in none_replaced).strip('()')
    else:
        if obj is None:
            return ''
        return str(obj)


def parse_datetime(datetime_string):
    """
    Parses `datetime_string` into Python `datetime.datetime` object and clears
    timezone information. If tz provided isn't UTC then raises `WrongDateTime`
    exception.

    :param datetime_string: string with time representation
    :rtype: datetime.datetime
    :return: corresponding date & time in UTC
    :raises iso8601.ParseError in case of string that doesn't suit any
        date/time representation
    :raises WrongDateTime in case datetime_string is provided in timezone
        different from UTC
    """
    dt = parse_date(datetime_string)
    if dt.tzinfo:
        if dt.tzinfo.utcoffset(dt):
            raise ValueError(datetime_string)
        # clear timezone info
        dt = dt.replace(tzinfo=None)
    return dt


def process_value(obj):
    if not isinstance(obj, str):
        return obj
    try:
        return parse_datetime(obj)
    except ParseError:
        return obj


class SQLAlchemySerializer(JSONEncoder):
    def default(self, o):
        if isinstance(o, (TypeEngine, VisitableType)):
            return str(o)
        if isinstance(o, set):
            return list(o)
        else:
            return JSONEncoder.default(self, o)


def random_str(length):
    """
    Generate random alphanumeric string with digits and letters

    :param length: length of output sequence
    """
    return ''.join(choice(string.ascii_letters + string.digits)
                   for _ in range(length))
