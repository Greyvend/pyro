from pyro import db
from pyro.tj import compose_tj_name
from pyro.utils import attribute_name


def _get_hierarchy(engine, relation_name, dimension):
    """
    Calculate list of attribute names in the order they should appear in the
    table.

    :param engine: SQLAlchemy engine to be used
    :param relation_name: name of relation to get hierarchy from
    :param dimension: configuration value of user defined dimension

    :return: list of attribute names in correct order. First attribute to be
    used as top level in hierarchy, the last one is bottom level
    """
    # Count amount of different values in each of attributes
    attributes = list(map(attribute_name, dimension['attributes']))
    counts = db.count_attributes(engine, relation_name, attributes)
    counted_attributes = zip(counts, attributes)
    sorted_attributes = sorted(counted_attributes)
    return list(zip(*sorted_attributes))[1]


def _build(engine, contexts, dimensions, measure):
    """
    Create internal representation of the cross table

    :param engine: SQLAlchemy engine to be used
    :param contexts: list of contexts (lists of relations)
    :param dimensions: configuration value of user defined dimensions
    :type dimensions: list of strings
    :param measure: name of the attribute to populate table body with
    :type measure: str

    :return: in-memory representation of the result table
    """
    cross_table = []
    assert len(contexts) == 3
    # section 1: build top part of the header (Y dimension)
    relation_y_name = compose_tj_name(contexts[0])
    hierarchy_y = _get_hierarchy(engine, relation_y_name, dimensions[0])
    projection_y = db.project(engine, relation_y_name, hierarchy_y)

    # section 2: build side part of the header (X dimension)
    relation_x_name = compose_tj_name(contexts[1])
    hierarchy_x = _get_hierarchy(engine, relation_x_name, dimensions[1])
    projection_x = db.project(engine, relation_x_name, hierarchy_x)

    # section 3: build table body (Z block)
    relation_z_name = compose_tj_name(contexts[2])
    body = []
    for row in projection_x:
        # TODO: same_rows_amount = count_equal(projection_x, i), i - row index
        body_row = []
        for col in projection_y:
            where_dict = {**row, **col}
            data_rows = db.get_data(engine, relation_z_name, [measure],
                                    constraint=where_dict)
            cell = tuple(row[measure] for row in data_rows)
            body_row.append(cell)
        # TODO: body_row.extend([body_row for x in range(same_rows_amount)])
        # TODO: same story with columns
        body.append(body_row)

    # section 4: finally assemble all 3 parts into single table view
    # transpose, fill empty cells and attribute names
    # section 4.1: transpose y projection to rows form
    list_projection_y = ([d[key] for key in hierarchy_y] for d in projection_y)
    transposed_y = zip(*list_projection_y)
    empty_y_cells = tuple('' for _ in range(len(hierarchy_x)))
    final_y = (empty_y_cells + (hierarchy_y[i],) + row
               for i, row in enumerate(transposed_y))

    content_len = len(body[0])
    x_measure_header = tuple(hierarchy_x) + ('',) + \
                        (measure,) * content_len
    list_projection_x = (tuple([x_row[key] for key in hierarchy_x] + [''] + body_row)
                         for x_row, body_row in zip(projection_x, body))
    # fill everything in cross_table
    cross_table.extend(final_y)
    cross_table.append(x_measure_header)
    cross_table.extend(list_projection_x)
    return cross_table


def _to_html(table):
    return '<html></html>'


def create(engine, contexts, dimensions, measure, file_name):
    # perform calculations and prepare HTML code
    table = _build(engine, contexts, dimensions, measure)
    html_table = _to_html(table)
    # write to a file
    with open(file_name, 'w') as html_file:
        html_file.write(html_table)
