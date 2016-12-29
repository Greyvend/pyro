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

        self.read_config(self._file)

    def read_config(self, file):
        """
        Load cache configuration from the file provided. Create empty file if
        it doesn't exist.

        :param file: path of the configuration file to use
        :type file: str
        """
        with open(file, 'a+') as config_file:
            config_file.seek(0)
            try:
                self._config = json.load(config_file)
            except JSONDecodeError:
                self._config = []
                json.dump(self._config, config_file)
        self._relation = None
        self.enabled = False

    def enable(self, context, constraint):
        """
        Activate cache functionality for the given context with logical
        constraint if there is cached data that has all required rows. Set
        enabled flag to True if the appropriate cache entry was found.

        :param context: list of relations in the join
        :param constraint: list of lists of predicates, representing logical
            constraint
        """
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

    def contains(self, constraint):
        """
        Check whether data satisfying given logical constraint exists in the
        cache.

        :param constraint: list of lists of predicates, representing logical
            constraint
        :return True if there's at least one row that satisfies the constraint,
            False otherwise
        """
        count = db.count_constrained(self._engine, self._relation['name'],
                                     constraint)
        return count > 0

    def add(self, relation, context, constraint):
        """
        Add given DB relation to the cache. context & constraint describe the
        relations used for join and logical restrictions the data has

        :param relation: dict describing DB table holding the data
        :param context: list of relations in the join
        :param constraint: list of lists of predicates, representing logical
            constraint
        """
        if relation in map(lambda entry: entry['relation'], self._config):
            return
        new_entry = {'relation': relation, 'context': context,
                     'constraint': constraint}
        self._config.append(new_entry)
        with open(self._file, 'w') as config_file:
            json.dump(self._config, config_file, cls=SQLAlchemySerializer,
                      ensure_ascii=False)

    def restore(self, dest_relation, *constraints):
        """
        Load the data from the cache to the dest_relation DB relation, whilst
        filtering by logical constraints provided.

        :param dest_relation: dict describing DB table to be restored into
        :param constraints: arbitrary amount of logical constraints that
            specify filtering options for the source data
        """
        db.insert_from_select(self._engine, dest_relation,
                              self._relation, *constraints)
