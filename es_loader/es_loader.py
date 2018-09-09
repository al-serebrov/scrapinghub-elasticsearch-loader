from elasticsearch import helpers
import logging


logger = logging.getLogger('es_loader')
logger.setLevel(level=logging.DEBUG)

# create console handler and set level to info
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# add formatter to console and file handlers
ch.setFormatter(formatter)

# add ch and fh to logger
logger.addHandler(ch)


class ESPipeline(object):

    def __init__(self, sc, es, job_id, index=None, doc_type='product'):
        self.sc = sc
        self.es = es
        self.job_id = job_id
        self.index_name = index
        self.doc_type = doc_type
        self.items_buffer = []
        self._create_index()

    def _create_index(self):
        if not self.index_name:
            self.index_name = self.job_id.replace('/', '_')
        if self.es.indices.exists(self.index_name):
            logger.warning('Index already exists, deleting it')
            self.es.indices.delete(self.index_name, ignore=[400, 404])
        self.es.indices.create(self.index_name)
        logger.debug('Index created')

    def _get_items(self):
        logger.debug('Getting items')
        items = self.sc.get_job(self.job_id).items.iter()
        return items

    def _index_item(self, item):
        index_action = {
            '_op_type': 'index',
            '_index': self.index_name,
            '_type': self.doc_type,
            'doc': dict(item)
        }

        self.items_buffer.append(index_action)

        if len(self.items_buffer) >= 500:
            self._send_items()
            self.items_buffer = []

    def _send_items(self):
        logger.debug('Bulk writing items')
        helpers.bulk(client=self.es, actions=self.items_buffer)

    def process_items(self):
        for item in self._get_items():
            self._index_item(item)
