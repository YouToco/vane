.PHONY: test-server test-web test-contract build-server

test-server:
	$(MAKE) -C server test

test-web:
	npm --prefix web test

test-contract:
	python3 -m unittest discover -s tests/contract -p 'test_*.py'

build-server:
	$(MAKE) -C server build
