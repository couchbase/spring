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
        'console_scripts': ['spring = spring.__main__:main']
    },
    install_requires=[
        'couchbase==1.2.0',
        'decorator',
        'logger',
        'numpy',
        'requests==2.1.0',
        'twisted==10.0.0',
    ]
)
