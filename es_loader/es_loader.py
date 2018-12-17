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
        self.loaded_items_count = 0
        self.buffer_size = 5000
        self._create_index()
        self._get_job()
        self._get_scraped_items_count()

    def _create_index(self):
        if not self.index_name:
            self.index_name = self.job_id.replace('/', '_')
        if self.es.indices.exists(self.index_name):
            logger.warning('Index already exists, deleting it')
            self.es.indices.delete(self.index_name, ignore=[400, 404])
        self.es.indices.create(self.index_name)
        logger.debug('Index created')

    def _get_job(self):
        self.job = self.sc.get_job(self.job_id)

    def _get_job_metadata(self):
        self.metadata = self.job.metadata.list()

    def _get_scraped_items_count(self):
        self._get_job_metadata()
        for data in self.metadata:
            if data[0] == 'scrapystats':
                self.items_count = data[1]['item_scraped_count']

    def _get_items(self):
        logger.debug(
            'There are {} items scraped. Downloading...'.format(self.items_count)
        )
        items = self.job.items.iter()
        return items

    def _index_item(self, item):
        if self.buffer_size == 0:
            return
        index_action = {
            '_op_type': 'index',
            '_index': self.index_name,
            '_type': self.doc_type,
            'doc': dict(item)
        }

        self.items_buffer.append(index_action)

        if len(self.items_buffer) >= self.buffer_size:
            self.loaded_items_count += len(self.items_buffer)
            self._send_items()
            self.items_buffer = []

        if len(self.items_buffer) < self.buffer_size \
                and (self.items_count - self.loaded_items_count) < self.buffer_size and self.buffer_size > 0:
            self.buffer_size = self.items_count - self.loaded_items_count
            self.loaded_items_count += len(self.items_buffer)
            if len(self.items_buffer) > 0:
                self._send_items()

    def _send_items(self):
        logger.debug('Bulk writing {} items'.format(len(self.items_buffer)))
        helpers.bulk(client=self.es, actions=self.items_buffer)

    def process_items(self):
        for item in self._get_items():
            self._index_item(item)
