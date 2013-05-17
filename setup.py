from setuptools import setup

setup(
    name='spring',
    version='1.2.0',
    description='Simple Couchbase workload generator based on pylibcouchbase',
    author='Pavel Paulau',
    author_email='pavel.paulau@gmail.com',
    packages=['spring'],
    entry_points={
        'console_scripts': ['spring = spring.main:main']
    },
    install_requires=[
        'argparse==1.2.1',
        'couchbase==0.9',
    ]
)
