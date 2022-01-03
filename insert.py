import argparse
import logging

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from data_reader import WikidataDumpReader

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO,
                    filename='./insert.log'
                    )

logger = logging.getLogger(__name__)


class ES_localdb():
    def __init__(self):
        self.name = 'es_based_entities_for_linking'
        self.doc_type_name = 'wiki_entities'
        self.index_name = 'wikidata_entity_linking'

    def build_es_index(self, filename):
        '''
        build an es index for all data.
        :return:
        '''

        es = Elasticsearch(['localhost'], port=9201)
        doc_type_name = 'wiki_entities'
        index_name = 'wikidata_entity_linking'

        es.indices.delete(index=index_name, ignore=[400, 404])
        es.indices.create(index=index_name, ignore=400)
        logger.info('****build index of %s.****' % index_name)
        dump = WikidataDumpReader(filename)
        bulk_num = 0

        with dump as reader:
            actions = []
            for idx, item in enumerate(reader):
                doc = item.to_document()
                if doc is None:
                    continue
                actions.append(
                    {"_index": index_name,
                     "_type": doc_type_name,
                     "_id": idx,
                     "_source": doc
                     })
                bulk_num = bulk_num + 1
                if bulk_num == 2000:
                    helpers.bulk(es, actions)
                    logger.info('finish insert %d entities to es.' % (idx))
                    actions = []
                    bulk_num = 0

            if len(actions) > 0:
                helpers.bulk(es, actions)
                logger.info("ALL FINISHED")
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', type=str, help='insert,build', default='insert')
    parser.add_argument('--database', type=str, default='wiki', help='wikidata')
    parser.add_argument('--datafile', type=str, default='./data/latest-all.json.bz2')
    args = parser.parse_args()
    runner = ES_localdb()
    runner.build_es_index(args.datafile)

if __name__ == '__main__':
    main()
