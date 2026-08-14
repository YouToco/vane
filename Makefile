.PHONY: quick test-server test-web test-contract test-release full release

quick:
	./ops/bin/vane quick --risk "$${RISK:-B}" --base "$${BASE:-origin/main}" --head "$${HEAD:-HEAD}"

test-server:
	$(MAKE) -C server test

test-web:
	npm --prefix web test

test-contract:
	python3 -m unittest discover -s tests/contract -p 'test_*.py'

test-release:
	python3 -m unittest discover -s ops/tests -p 'test_*.py'

full:
	./ops/bin/vane full --sha "$${SHA:-$$(git rev-parse HEAD)}"

release:
	test -n "$(SHA)"
	./ops/bin/vane release --sha "$(SHA)"
