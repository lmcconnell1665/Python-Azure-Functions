# source ~/.venv/bin/activate
setup:
	python3 -m venv ~/.venv

install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

lint:
	flake8 mcdevtools/**/*.py
	flake8 --ignore E712 tests/**/*.py

test:
	python3 setup.py pytest

build:
	python3 setup.py bdist_wheel