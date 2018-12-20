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

    def __init__(
            self, sc, es, job_id, index=None, doc_type='product',
            base_buffer_size=5000, max_buffer_size=20000):
        """Constructor.

        Arguments:
            sc - an instance of ScrapinghubClient created with API key.
            es - an instance of Elasticsearh created with ES URI.
            job_id - a string with job_id from Scrapy Cloud in format of
                orgranization/project/job, e.g.: '1886/5454/43'
            index - a string with ElasticSearch index name, defaults to None
            doc_type - a string with ElasticSearch document type name
            base_buffer_size - integer with base buffer size which would be a
                starting point for buffer size calculation
            max_buffer_size - integer with maximal buffer size allowed, note
                that it should be evenly devisible by base buffer size, i.e.
                max_buffer_size % base_buffer_size = 0
                Default limit is 20k, but if you have a lot of available RAM
                it could be as high as you need it to.
        """
        self.sc = sc
        self.es = es
        self.job_id = job_id
        self.index_name = index
        self.doc_type = doc_type
        self.loaded_items_count = 0
        self.base_buffer_size = 5000
        self.max_buffer_size = 20000
        self._create_index()

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

    def _get_items(self):
        """Get items from Scrapy Cloud.

        Besides getting items this method also gets job metadata,
        extracts items count and calls method to calculate buffer size.

        Returns:
            items - an iterator with items.

        Raises:
            ValueError - if the method is unable to get items count.
        """
        job = self.sc.get_job(self.job_id)
        metadata = job.metadata.list()

        items_count = 0
        for data in metadata:
            if data[0] == 'scrapystats':
                items_count = data[1]['item_scraped_count']

        if not items_count:
            raise ValueError('Unable to get items count for job stats')

        logger.debug(
            'There are {} items scraped. Downloading...'.format(items_count)
        )

        self._calculate_buffer_size(items_count, self.base_buffer_size)
        logger.debug('Batch size is set to {}'.format(self.buffer_size))

        items = job.items.iter()
        return items

    def _calculate_buffer_size(self, items_count, base_buffer_size):
        """Calculate buffer size.

        Hardcoded buffer size doesn't work as there might be jobs with
        very different amount of items - from 100 to 100k, and it's better
        to make it more adjustable.
        If the job items count is smaller than base buffer size, the
        method will use number from base_buffer_size attribute as buffer size,
        and otherwise - if items count is much greater than base buffer size,
        the method starts enlarging it, but it's limited by max_buffer_size
        attibute - this is done to avoid having huge batches
        to be processed in memory.

        Arguments:
            items_count - integer with job items count
            base_buffer_size - starting point for buffer size calculation.
        """
        buf = items_count // base_buffer_size
        if buf > 5 and base_buffer_size < self.max_buffer_size:
            self._calculate_buffer_size(items_count, base_buffer_size * 2)
        else:
            self.buffer_size = base_buffer_size

    def process_items(self):
        """Entry point to the whole process.
        Calls all methods in order to process items of the job.
        """
        batch = []
        for item in self._get_items():

            index_action = {
                '_op_type': 'index',
                '_index': self.index_name,
                '_type': self.doc_type,
                'doc': dict(item)
            }

            batch.append(index_action)

            # if we accumulated enough items, send them in a bulk:
            if len(batch) >= self.buffer_size:
                self._bulk_send_items(batch)
                batch = []

        # If there are any items left, send them in a bulk:
        if len(batch) > 0:
            self._bulk_send_items(batch)

    def _bulk_send_items(self, batch):
        self.loaded_items_count += len(batch)
        logger.debug('Bulk writing {} items'.format(len(batch)))
        helpers.bulk(client=self.es, actions=batch)
