import json

from pyro.constraints.operations import is_domain_included


class Cache:
    def __init__(self, engine, file):
        self._engine = engine
        self._file = file
        with open(file) as config_file:
            self._config = json.load(config_file)
        self._relation = None
        self.enabled = False

    def enable(self, context, constraint):
        for entry in self._config:
            context_names = map(lambda r: r['name'], context)
            entry_names = map(lambda r: r['name'], entry['context'])
            context_supersets = set(context_names).issubset(entry_names)
            if not context_supersets:
                continue
            domain_included = is_domain_included(constraint,
                                                 entry['constraint'])
            if domain_included:
                self._relation = entry['relation']
                self.enabled = True
                return

    def add(self, relation, context, constraint):
        if relation in map(lambda entry: entry['relation'], self._config):
            return
        new_entry = {'relation': relation, 'context': context,
                     'constraint': constraint}
        self._config.append(new_entry)
        with open(self._file, 'w') as config_file:
            json.dump(self._config, config_file)

    def restore(self, dest_relation, *constraints):
        pass
