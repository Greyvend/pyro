import json
from json import JSONDecodeError

from pyro import db
from pyro.constraints.operations import is_domain_included, equal
from pyro.utils import SQLAlchemySerializer


class Cache:
    """
    Class implementing register of cached Tables of Joins. It also has a state
    of enabled TJ that can be used to restore data into some other table.
    """
    def __init__(self, engine, file):
        self._engine = engine
        self._file = file
        self._config = None
        self._active_entry = None
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
        self._active_entry = None
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
                self._active_entry = entry
                self.enabled = True
                return

    def full_match(self, context, constraint):
        for entry in self._config:
            context_names = map(lambda r: r['name'], context)
            entry_names = map(lambda r: r['name'], entry['context'])
            contexts_equal = set(entry_names) == set(context_names)
            if not contexts_equal:
                continue
            if equal(constraint, entry['constraint']):
                return entry['relation']
        return None

    def contains(self, constraint):
        """
        Check whether data satisfying given logical constraint exists in the
        cache.

        :param constraint: list of lists of predicates, representing logical
            constraint
        :return True if there's at least one row that satisfies the constraint,
            False otherwise
        """
        count = db.count_constrained(self._engine,
                                     self._active_entry['relation']['name'],
                                     constraint)
        return count > 0

    def contains_context(self, context):
        """
        Define whether given context is contained in the cache. If lossless
        join property isn't satisfied the results will be wrong.

        :param context: list of relations satisfying lossless join property
        :return: True if the context is a subset of active cache entry's
            context, False otherwise
        """
        context_names = [r['name'] for r in context]
        self_context_names = [r['name'] for r in self._active_entry['context']]
        return set(context_names).issubset(self_context_names)

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
                              self._active_entry['relation'], *constraints)
