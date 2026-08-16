.PHONY: install run test lint

install:
	python3 -m pip install -r requirements.txt

run:
	python3 -m uvicorn scheduler.api:app --reload --port 8000

test:
	python3 -m pytest tests/ -v

lint:
	python3 -m ruff check scheduler/ tests/
