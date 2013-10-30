from setuptools import setup

from spring.version import VERSION


setup(
    name='spring',
    version=VERSION,
    description='Simple Couchbase workload generator based on pylibcouchbase',
    author='Pavel Paulau',
    author_email='pavel.paulau@gmail.com',
    packages=['spring'],
    entry_points={
        'console_scripts': ['spring = spring.main:main']
    },
    install_requires=[
        'argparse',
        'couchbase==1.1.0',
        'decorator',
        'logger'
    ]
)
