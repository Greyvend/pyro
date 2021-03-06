from itertools import groupby

from mako.template import Template

from pyro import db
from pyro.utils import xstr


def _get_hierarchy(engine, relation_name, attributes):
    """
    Calculate list of attribute names in the order they should appear in the
    table.

    :param engine: SQLAlchemy engine to be used
    :param relation_name: name of relation to get hierarchy from
    :param attributes: attributes to order in hierarchy

    :return: list of attribute names in correct order. First attribute to be
    used as top level in hierarchy, the last one is bottom level
    """
    counts = db.count_attributes(engine, relation_name, attributes)
    counted_attributes = zip(counts, attributes)
    sorted_attributes = sorted(counted_attributes)
    return list(zip(*sorted_attributes))[1]


def _prettify(hierarchy_y, y, hierarchy_x, x, body,
              measure):
    """
    Assemble all the input parts of the cross table adding necessary spaces
    and attribute names

    :param hierarchy_y: ordered names of the attributes in the Y dimension
    :param y: attribute values in Y dimension
    :param hierarchy_x: ordered names of the attributes in the X dimension
    :param x: attribute values in X dimension
    :param body: table contents
    :param measure: name of the measure attribute

    :return generator expression yielding rows of the final table
    """
    y_list = ([d[key] for key in hierarchy_y] for d in y)
    transposed_y = zip(*y_list)
    empty_y_cells = tuple('' for _ in range(len(hierarchy_x)))
    final_y = (empty_y_cells + (hierarchy_y[i],) + row
               for i, row in enumerate(transposed_y))

    content_len = len(body[0])
    x_measure_header = tuple(hierarchy_x) + ('',) + \
                       (measure,) * content_len
    final_x = (tuple([x_row[key] for key in hierarchy_x] + [''] +
                     body_row)
               for x_row, body_row in zip(x, body))
    for row_y in final_y:
        yield row_y
    yield x_measure_header
    for row_x in final_x:
        yield row_x


def _build(engine, relation_names, dimensions, measure):
    """
    Create internal representation of the cross table

    :param engine: SQLAlchemy engine to be used
    :param relation_names: lists of names of relations to be used
    :param dimensions: user defined dimensions
    :type dimensions: list of lists of strings
    :param measure: name of the attribute to populate table body with
    :type measure: str

    :return: in-memory representation of the result table
    """
    assert len(relation_names) == 3
    # section 1: build top part of the header (Y dimension)
    relation_y_name = relation_names[0]
    hierarchy_y = _get_hierarchy(engine, relation_y_name, dimensions[0])
    projection_y = db.project(engine, relation_y_name, hierarchy_y)
    merged_projection_y = list(map(lambda g: g[0], groupby(projection_y)))

    # section 2: build side part of the header (X dimension)
    relation_x_name = relation_names[1]
    hierarchy_x = _get_hierarchy(engine, relation_x_name, dimensions[1])
    projection_x = db.project(engine, relation_x_name, hierarchy_x)
    merged_projection_x = list(map(lambda g: g[0], groupby(projection_x)))

    # section 3: build table body (Z block)
    relation_z_name = relation_names[2]
    body = []
    for row in merged_projection_x:
        body_row = []
        for col in merged_projection_y:
            where_dict = {**row, **col}
            data_rows = db.get_data(engine, relation_z_name, [measure],
                                    constraint=where_dict)
            cell = tuple(row[measure] for row in data_rows)
            body_row.append(cell)
        body.append(body_row)

    # section 4: finally assemble all 3 parts into single table view
    return _prettify(hierarchy_y, merged_projection_y, hierarchy_x,
                     merged_projection_x, body, measure)


def _group_cells(table, max_col):
    column_list = list(zip(*table))
    res = []
    for col_num in range(max_col):
        cell_group = {}
        for k, group in groupby(column_list[col_num]):
            g = list(group)
            cell_group[g[0]] = len(g)
        res.append(cell_group)
    return res


def _to_html(table, dimensions):
    _table = list(table)
    len_y = len(dimensions[0])
    len_x = len(dimensions[1])

    table_tag = ['<table>']
    row_format = '<tr>{}</tr>'
    cell_format = '<td>{}</td>'
    cell_header_format = '<th>{}</th>'
    cell_measure_format = '<td class="measure">{}</td>'
    # section 1: build top header (Y part)
    cell_colspan_format = '<td colspan="{}">{}</td>'
    table_content = []
    for row in _table[:len_y]:
        cell_list = []
        for k, group in groupby(row):
            cell_group = list(group)
            group_len = len(cell_group)
            if len(cell_group) > 1:
                cell_str = cell_colspan_format.format(group_len, cell_group[0])
            else:
                if cell_group[0] in dimensions[0]:
                    _format = cell_header_format
                else:
                    _format = cell_format
                cell_str = _format.format(cell_group[0])
            cell_list.append(cell_str)
        row_str = row_format.format(''.join(cell_list))
        table_content.append(row_str)

    x_measure = (cell_header_format.format(c) if c in dimensions[1]
                 else cell_measure_format.format(c) for c in _table[len_y])
    x_measure_row_str = row_format.format(''.join(x_measure))
    table_content.append(x_measure_row_str)

    # section 2: build left header (X part) and body
    cell_rowspan_format = '<td rowspan="{}">{}</td>'
    cell_groups = _group_cells(_table[len_y + 1:], max_col=len_x)
    for local_row_num, row in enumerate(_table[len_y + 1:]):
        row_num = len_y + 1 + local_row_num
        cell_list = []
        # fill left header (X part)
        for col_num, cell in enumerate(row[:len_x]):
            previous_row = _table[row_num - 1]
            cell_appearances = cell_groups[col_num][cell]
            if cell == previous_row[col_num]:
                cell_str = ''
            elif cell_appearances > 1:
                cell_str = cell_rowspan_format.format(cell_appearances, cell)
            else:
                cell_str = cell_format.format(cell)
            cell_list.append(cell_str)
        # fill body
        for cell in row[len_x:]:
            cell_str = cell_format.format(xstr(cell))
            cell_list.append(cell_str)
        row_str = row_format.format(''.join(cell_list))
        table_content.append(row_str)
    table_tag.extend(table_content)
    table_tag.append('</table>')
    return ''.join(table_tag)


def create(engine, relation_names, dimensions, measure, file_name):
    # perform calculations and prepare HTML code
    table = _build(engine, relation_names, dimensions, measure)
    html_table = _to_html(table, dimensions)
    # write to a file
    template = Template(filename='templates/cross_table.html')
    with open(file_name, 'w') as html_file:
        html = template.render(table=html_table)
        html_file.write(html)
