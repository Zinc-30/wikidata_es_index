# wikidata_es_index

1. download latest [Wikidata](https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2) file in the data folder.
2. download the [elasticsearch software](https://www.elastic.co/cn/downloads/elasticsearch) and install it.
3. install the python package *elasticsearch* 

(make sure the version is suitable for the elasticsearch software installed in step 2)
```
pip install elasticsearch
```
4. run insert.py 
```
python insert.py
```
