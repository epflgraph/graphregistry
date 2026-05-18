echo "Running unit tests..."
pytest tests/unit_tests/*
echo "Running integration tests..."
pytest tests/integration_tests/*
echo "Done running tests."