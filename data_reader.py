import bz2
import json
import sys

class WikidataItemDocument(object):
    def __init__(self, json):
        self.json = json

    def get(self, field, default_value=None):
        return self.json.get(field, default_value)

    def __repr__(self):
        return '<WikidataItemDocument {}>'.format(self.json.get('id') or '(unknown qid)')

    def __iter__(self):
        return self.json.__iter__()

    def get_outgoing_edges(self, include_p31=True):
        """
        Given a JSON representation of an item,
        return the list of outgoing edges,
        as integers.
        """
        claims = self.get('claims', {})
        res = []
        for pid, pclaims in claims.items():

            if pid == 'P31' and not include_p31:
                continue
            for c in pclaims:
                try:
                    if c['mainsnak']['datatype'] == 'wikibase-item':
                        res.append((pid, c['mainsnak']['datavalue']['value']['id']))
                    if c['mainsnak']['datatype'] == 'quantity':
                        res.append((pid, c['mainsnak']['datavalue']['value']['amount']))
                    if c['mainsnak']['datatype'] == 'time':
                        res.append((pid, c['mainsnak']['datavalue']['value']['time']))
                except (KeyError, TypeError):
                    pass
        return res

    def get_nb_statements(self):
        """
        Number of claims on the item
        """
        nb_claims = 0
        for pclaims in self.get('claims', {}).values():
            nb_claims += len(pclaims)
        return nb_claims

    def get_nb_sitelinks(self):
        """
        Number of sitelinks on this item
        """
        return len(self.get('sitelinks', []))

    def get_types(self, pid='P31'):
        """
        Values of P31 claims
        """
        type_claims = self.get('claims', {}).get(pid, [])
        type_qids = [
            claim.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id')
            for claim in type_claims
        ]
        valid_type_qids = [qid for qid in type_qids if qid]
        return valid_type_qids

    def get_default_label(self, language):
        """
        English label if provided, otherwise any other label
        """
        labels = self.get('labels', {})
        preferred_label = labels.get(language, {}).get('value')
        if preferred_label:
            return preferred_label
        enlabel = labels.get('en', {}).get('value')
        if enlabel:
            return enlabel
        for other_lang in labels:
            return labels.get(other_lang, {}).get('value')
        return None

    def get_all_terms(self):
        """
        All labels and aliases in all languages, made unique
        """
        all_labels = {
            label['value']
            for label in self.get('labels', {}).values()
        }
        for aliases in self.get('aliases', {}).values():
            all_labels |= {alias['value'] for alias in aliases}
        return all_labels

    def get_aliases(self, lang):
        aliases = [
            alias['value']
            for alias in self.get('aliases', {}).get(lang, [])
        ]
        return aliases

    def get_identifiers(self, pid):
        # Fetch GRID
        id_claims = self.get('claims', {}).get(pid, [])
        ids = [
            claim.get('mainsnak', {}).get('datavalue', {}).get('value', {})
            for claim in id_claims
        ]
        valid_ids = [id for id in ids if id]
        return valid_ids

    def to_document(self):
        """
        Given a Wikibase entity, translate it to a es document for indexing.
        :returns: None if the entity should be skipped
        """
        enlabel = self.get_default_label('en')
        zhlabel = self.get_default_label('zh-cn')
        endesc = self.get('descriptions', {}).get('en', {}).get('value')
        zhdesc = self.get('descriptions', {}).get('zh-cn', {}).get('value')
        if not enlabel:
            return

        # Fetch aliases
        aliases = self.get_all_terms()

        # Edges
        edges = self.get_outgoing_edges()

        # Stats
        nb_statements = self.get_nb_statements()
        nb_sitelinks = self.get_nb_sitelinks()

        # types

        types = self.get_types()

        return {'id': self.get('id'),
                'revid': self.get('lastrevid') or 1,
                'label': enlabel,
                'zhlabel': zhlabel or '',
                'desc': endesc or '',
                'zhdesc': zhdesc or '',
                'edges': edges,
                'types': list(types),
                'aliases': list(aliases),
                'nb_statements': nb_statements,
                'nb_sitelinks': nb_sitelinks}


class WikidataDumpReader(object):
    """
    Generates a stream of `WikidataItemDocument` from
    a Wikidata dump.
    """

    def __init__(self, fname):
        self.fname = fname
        if fname == '-':
            self.f = sys.stdin
        else:
            self.f = bz2.open(fname, mode='rt', encoding='utf-8')

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        if self.fname != '-':
            self.f.close()

    def __iter__(self):
        for line in self.f:
            try:
                # remove the trailing comma
                if line.rstrip().endswith(','):
                    line = line[:-2]
                item = json.loads(line)
                yield WikidataItemDocument(item)
            except ValueError as e:
                # Happens at the beginning or end of dumps with '[', ']'
                continue


if __name__ == '__main__':
    filename = './data/latest-all.json.bz2'
    dump = WikidataDumpReader(filename)
    with dump as reader:
        actions = []
        for idx, item in enumerate(reader):
            print(item.to_document())
            break
