from setuptools import setup

setup(
    name='spring',
    version='1.0',
    description='Simple Couchbase workload generator based on pylibcouchbase',
    author='Pavel Paulau',
    author_email='pavel.paulau@gmail.com',
    packages=['spring'],
    entry_points={
        'console_scripts': ['spring = spring.main:main']
    },
    install_requires=['couchbase==0.9']
)
