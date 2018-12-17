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
    """ElasticSearch pipeline to load items to ElasticSearch instance.

    Use this class to load items downloaded from ScrapingHub API to
    ElasticSearch instance. Items are lazy-loaded using SH API, split
    into batches and loaded to ElasticSearch.
    """

    def __init__(self, sc, es, job_id, index=None, doc_type='product'):
        """Constructor.

        Arguments:
            sc - an instance of ScrapinghubClient created with API key.
            es - an instance of Elasticsearh created with ES URI.
            job_id - a string with job_id from Scrapy Cloud in format of
                orgranization/project/job, e.g.: '1886/5454/43'
            index - a string with ElasticSearch index name, defaults to None
            doc_type - a string with ElasticSearch document type name
        """
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
        """Create index in the ElasticSearch.

        If index name is not set in the constructor, the method uses
        job ID to compose an index name by replacing slashes to underscores.
        For example, for the job with ID '1886/5454/43' the resulting index
        name would be 1886_5454_43.
        Also, this method checks if the index already exists and deletes it.

        TODO:
            - add the parameter to update index, not recreate it if needed
        """
        if not self.index_name:
            self.index_name = self.job_id.replace('/', '_')
        if self.es.indices.exists(self.index_name):
            logger.warning('Index already exists, deleting it')
            self.es.indices.delete(self.index_name, ignore=[400, 404])
        self.es.indices.create(self.index_name)
        logger.debug('Index created')

    def _get_job(self):
        """Get job from ScrapingHubClient istance."""
        self.job = self.sc.get_job(self.job_id)

    def _get_job_metadata(self):
        """Get job metadata."""
        self.metadata = self.job.metadata.list()

    def _get_scraped_items_count(self):
        """Get scraped items count from job metadata.

        The count is needed to make log messages more verbose
        and to calculate batches size"""
        self._get_job_metadata()
        for data in self.metadata:
            if data[0] == 'scrapystats':
                self.items_count = data[1]['item_scraped_count']

    def _get_items(self):
        """Get items from Scrapy Cloud.

        Returns:
            items - an iterator with items.
        """
        logger.debug(
            'There are {} items scraped. Downloading...'.format(self.items_count)
        )
        items = self.job.items.iter()
        return items

    def _index_item(self, item):
        """Prepare item to be indexed to ElasticSearch.

        This method wrapps item from Scrapy Cloud to the be ready
        to be indexed into ElasticSearch.
        Also here actual batching is happening - items are accumulated
        into buffer and then sent to ElasticSearh in bulk.

        Arguments:
            item - an item from items() iterator of ScrapingHubClient.Job
        """
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
        """Bulk write items to ElasticSearch."""
        logger.debug('Bulk writing {} items'.format(len(self.items_buffer)))
        helpers.bulk(client=self.es, actions=self.items_buffer)

    def process_items(self):
        """Entry point to the whole process.

        Calls all methods in order to process items of the job.
        """
        for item in self._get_items():
            self._index_item(item)
