.PHONY: build

ENV := env

build: ext
	virtualenv -p python2.7 ${ENV}
	${ENV}/bin/pip install -r requirements.txt

ext:
	gcc -g -fwrapv -O2 -Wall -Wstrict-prototypes -shared -O2 -fPIC \
		-lpython2.7 \
		-I/usr/include/python2.7 \
		-o fastdocgen.so src/fastdocgen.c

test:
	${ENV}/bin/python tests.py -v

bench:
	${ENV}/bin/python benchmark.py
	gprof2dot -f pstats benchmark.prof | dot -Tsvg -o benchmark.svg

flake8:
	${ENV}/bin/flake8 --statistics spring *.py

clean:
	rm -fr build dist spring.egg-info fastdocgen.so benchmark.prof benchmark.svg env

pypi:
	${ENV}/bin/python setup.py build sdist upload
