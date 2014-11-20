.PHONY: test

clean:
	rm -rf node_modules

install:
	npm install

test: clean install
	npm test
