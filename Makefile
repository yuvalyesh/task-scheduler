.PHONY: run test lint

run:
	uvicorn scheduler.api:app --reload --port 8000

test:
	pytest tests/ -v

lint:
	ruff check scheduler/ tests/
