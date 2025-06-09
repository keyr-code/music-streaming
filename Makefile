.PHONY: test test-unit test-integration test-coverage clean

# Run all tests
test:
	python -m pytest

# Run only unit tests
test-unit:
	python -m pytest music_streaming/tests/unit/

# Run only integration tests
test-integration:
	python -m pytest music_streaming/tests/integration/

# Run tests with coverage report
test-coverage:
	python -m pytest --cov=music_streaming music_streaming/tests/

# Clean up cache files
clean:
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +