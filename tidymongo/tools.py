import pandas as pd
from collections import defaultdict
from copy import deepcopy


class TidyResult(object):

    def __init__(self, observational_unit):
        self.observational_unit = observational_unit
        self.collection_id = '{}_id'.format(self.observational_unit)
        self.ref_tables_ = defaultdict(list)

    @property
    def tables(self):
        return self.ref_tables_.keys()

    @property
    def __ref_tables(self):
        tables = dict()
        for k, v in self.ref_tables_.iteritems():
            df = pd.DataFrame(data=v)
            if '_id' in df.columns:
                df.set_index('_id', inplace=True)
            tables[k] = df
        return tables

    def add_document(self, doc_type, document):
        if isinstance(document, list):
            self.ref_tables_[doc_type].extend(document)
        else:
            self.ref_tables_[doc_type].append(document)

    def merge(self, other):
        return merge(self, other)

    def __getattr__(self, name):
        if name in self.__ref_tables.keys():
            return self.__ref_tables[name]
        else:
            raise AttributeError()

    def __repr__(self):
        return "TidyResult(tables={})".format(self.tables)


def tidy(data, observational_unit, schema='infer'):

    documents = deepcopy(data)

    from collections import OrderedDict
    results = OrderedDict()

    results[observational_unit] = TidyResult(observational_unit)

    for document in documents:

        try:
            foreign_key = document['_id']
        except KeyError:
            foreign_key = None

        observation_vars = []
        for k, v in document.iteritems():
            if isinstance(v, list) or isinstance(v, dict):

                tr = results.get(k, TidyResult(observational_unit=k))

                if foreign_key:
                    # TODO: right now observational_unit is plural, might want to make it singular is "_id"
                    tr.add_document(k, add_foreign_key(v, observational_unit, foreign_key))
                else:
                    tr.add_document(k, v)

                results[k] = tr
            else:
                observation_vars.append(k)

        results[observational_unit].add_document(observational_unit, {k: document[k] for k in observation_vars})

    return reduce(lambda a, b: a.merge(b), results.values())


def merge(left, right):
    results = left
    for k, v in right.ref_tables_.iteritems():
        if k == right.observational_unit:
            key = k
        else:
            key = '{0}_{1}'.format(right.observational_unit, k)
        results.add_document(key, v)
    return results


def add_foreign_key(data, key_name, key_value):
    if isinstance(data, list):
        return [add_foreign_key(d, key_name, key_value) for d in data]
    elif isinstance(data, dict):
        d = deepcopy(data)
        d[key_name] = key_value
        return d
    else:
        raise TypeError('Unrecognized data type. '
                        'Must be list of dict.')
