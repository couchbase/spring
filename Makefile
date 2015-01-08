build: ; \
    gcc -g -fwrapv -O2 -Wall -Wstrict-prototypes -shared -O2 -fPIC -lpython \
    -o fastdocgen.so src/fastdocgen.c \
    -I/usr/include/python2.7

test: ; \
    python tests.py -v

bench: ; \
    python benchmark.py; \
    gprof2dot -f pstats benchmark.prof | dot -Tsvg -o benchmark.svg

pep8: ; \
    pep8 --ignore E501 spring tests.py benchmark.py

clean: ; \
    rm -fr build dist spring.egg-info fastdocgen.so benchmark.prof benchmark.svg

pypi: ; \
    python setup.py build sdist upload
