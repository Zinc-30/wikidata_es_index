from elasticsearch import Elasticsearch


class wikidataSearch():
    def __init__(self, num=10):
        self.k = num
        self.es = Elasticsearch(['localhost'], port=9201)

    def get_entity_by_alias(self, mention):
        """
        return the top k(default=10) entity for alias mention.
        :param mention: string
        :return: entity list
        """
        query = {"match": {"aliases": {"query": mention, "operator": "and"}}}
        response = self.es.search(index='wikidata_entity_linking', query=query, size=self.k)
        try:
            entities = [x['_source'] for x in response['hits']['hits']]
            return entities
        except:
            return []

    def get_entity_by_name(self, name):
        query = {"match": {"label": {"query": name, "operator": "and"}}}
        response = self.es.search(index='wikidata_entity_linking', query=query, size=self.k)
        try:
            entities = [x['_source'] for x in response['hits']['hits']]
            return entities
        except:
            return []

    def get_entity_by_id(self, id):
        query = {"match": {"id": {"query": id, "operator": "and"}}}
        response = self.es.search(index='wikidata_entity_linking', query=query, size=self.k)
        try:
            entity = [x['_source'] for x in response['hits']['hits']]
            return entity
        except:
            return []

    def get_entity_edges(self, entity):
        """
        return the entity edges
        :param entity: json dict
        :return: a list of pair [(predicate,object)...]
        """
        return entity['edges']


if __name__ == '__main__':
    ws = wikidataSearch(100)
    for x in ws.get_entity_by_id('P31'):
        print(x)
