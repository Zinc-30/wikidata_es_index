import argparse

from elasticsearch import Elasticsearch


class ES_localdb():
    def __init__(self):
        self.name = 'es_based_entities_for_linking'
        self.doc_type_name = 'wiki_entities'
        self.index_name = 'wikidata_entity_linking'

    def es_search_query(self, query):
        '''
        search the top k(default=10) document for query on the field of query_type.
        '''
        es = Elasticsearch(['localhost'], port=9201)
        predication = dict()
        dsl = {'query': {'match': {'aliases': query}}}
        result_1 = es.search(index=self.index_name, doc_type=self.doc_type_name, body=dsl)
        hitted = min(len(result_1['hits']['hits']), 50)
        for idx in range(hitted):
            # try:
            predication[result_1['hits']['hits'][idx]['_source']['title']] = {
                "text": result_1['hits']['hits'][idx]['_source']['text'],
                "merge_triples": result_1['hits']['hits'][idx]['_source']['merge_triples'],
                "original_triples": result_1['hits']['hits'][idx]['_source']['original_triples']}

        return predication


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', type=str, help='insert,build', default='insert')
    parser.add_argument('--database', type=str, default='wiki', help='wiki, dbpedia')
    parser.add_argument('--profile_name', type=str, default='profiles/human_organization_location.json')
    parser.add_argument('--datafile', type=str, default='./data/latest-all.json.bz2')
    args = parser.parse_args()

    runner = ES_localdb()
    if args.operation == 'insert' and args.database == 'wiki':
        runner.build_es_index(args.profile_name, args.datafile)


if __name__ == '__main__':
    main()
