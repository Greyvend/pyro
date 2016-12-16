def not_null(attributes):
    return [[{'attribute': a, 'operator': '<>', 'value': None}
             for a in attributes]]
