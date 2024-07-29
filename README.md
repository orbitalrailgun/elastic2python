# elastic2python

elastic2python это набор функций, которые позволяют удобно извлечь данные из elastic или opensearch с помощью python и представить их в виде list of dicts. Это сильно упростит работу с данными через pandas.

Для работы необходимы:

from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
import copy
