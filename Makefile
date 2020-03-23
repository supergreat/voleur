.PHONY: install clean lint typecheck

install:
	pip install -r requirements.txt

clean:
	$(shell find . -name "*.pyc" -o -name "__pycache__" | xargs rm -rf)

lint:
	flake8 ./voleur

typecheck:
	mypy ./voleur
