def not_null(attributes):
    return [[{'attribute': a, 'operation': '<>', 'value': None}
             for a in attributes]]
