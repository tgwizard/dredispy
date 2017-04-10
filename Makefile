.PHONY: setup install flake8 test serve

VIRTUAL_ENV?=./venv

setup:
	virtualenv --python=python3.6 venv
	$(MAKE) install

install:
	$(VIRTUAL_ENV)/bin/pip install -r requirements.txt

flake8:
	$(VIRTUAL_ENV)/bin/flake8 .

test: flake8
	$(VIRTUAL_ENV)/bin/coverage run --source dredispy -m py.test tests/
	$(VIRTUAL_ENV)/bin/coverage report -m

serve:
	$(VIRTUAL_ENV)/bin/python -m dredispy.run
