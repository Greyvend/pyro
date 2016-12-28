import json
from json import JSONDecodeError

from pyro import db
from pyro.constraints.operations import is_domain_included
from pyro.utils import SQLAlchemySerializer


class Cache:
    def __init__(self, engine, file):
        self._engine = engine
        self._file = file
        self._config = None
        self._relation = None
        self.enabled = False

        self.read_config()

    def read_config(self):
        with open(self._file, 'a+') as config_file:
            config_file.seek(0)
            try:
                self._config = json.load(config_file)
            except JSONDecodeError:
                self._config = []
                json.dump(self._config, config_file)
        self._relation = None
        self.enabled = False

    def enable(self, context, constraint):
        for entry in self._config:
            context_names = map(lambda r: r['name'], context)
            entry_names = map(lambda r: r['name'], entry['context'])
            context_supersets = set(entry_names).issubset(context_names)
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
            json.dump(self._config, config_file, cls=SQLAlchemySerializer,
                      ensure_ascii=False)

    def restore(self, dest_relation, *constraints):
        db.insert_from_select(self._engine, dest_relation,
                              self._relation, *constraints)
