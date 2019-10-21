"""A setuptools based setup module.
"""

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='shes',
    version='0.5',
    description='Load items from Scrapy Cloud do ElasticSearch',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/al-serebrov/scrapinghub-elasticsearch-loader',
    author='Alexander Serebrov',
    author_email='serebrov.alexandr@gmail.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='scrapy cloud scrapinghub elasticsearch loader',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    python_requires='>=3.5',
    instal_requires=[
        'docopt==0.6.2',
        'elasticsearch==6.3.1',
        'msgpack-python==0.5.6',
        'requests==2.21.0',
        'scrapinghub==2.0.3',
        'six==1.11.0',
        'flake8'
    ]
)
