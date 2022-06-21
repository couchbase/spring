from setuptools import setup, Extension

from spring.version import VERSION

fastdocgen = Extension('fastdocgen', sources=['src/fastdocgen.c'])


setup(
    name='spring',
    version=VERSION,
    description='Simple Couchbase workload generator based on pylibcouchbase',
    packages=['spring'],
    entry_points={
        'console_scripts': ['spring = spring.__main__:main']
    },
    install_requires=[
        'couchbase==2.1.0',
        'decorator==4.0.10',
        'logger==1.4',
        'numpy==1.22.0',
        'requests==2.1.0',
        'urllib3==1.10.4',
        'twisted==16.2.0',
        'dcp-client'
    ],
    dependency_links=[
        'git+https://github.com/couchbaselabs/python-dcp-client.git#egg=dcp-client'
    ],
    ext_modules=[
        fastdocgen
    ],
)
