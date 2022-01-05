# wikidata_es_index
## Insert the wikidata KB into elasticsearch
1.  download latest [Wikidata](https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2) file in the data folder.
2.  download the [elasticsearch software](https://www.elastic.co/cn/downloads/elasticsearch) and install it.
3.  install the python package *elasticsearch* 
(make sure the version is suitable for the elasticsearch software installed in step 2)
    ```
    pip install elasticsearch
    ```
4.  run insert.py 
    ```
    python insert.py
    ```
    
## Search in the wikidata
you can direct search with search.py
```
    from search import *
    ws = wikidataSearch(100)
    for x in ws.get_entity_by_id('P31'):
        print(x)
```
or

write your own query
```
from elasticsearch import Elasticsearch
es = Elasticsearch(['localhost'], port=9201)
query = {"match": {"label": {"query": name}}}
response = es.search(index='wikidata_entity_linking', query=query)
try:
    entities = [x['_source'] for x in response['hits']['hits']]
    return entities
except:
    return []
```
please check the [API doc](https://elasticsearch-py.readthedocs.io/en/v7.16.2/api.html#elasticsearch) and [query doc](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html) for how to write the query 

