from setuptools import setup, find_packages

setup(
    name="music_streaming",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "confluent-kafka",
        "pandas",
        "faker",
        "minio",
        "psycopg2-binary",
        "pyspark",
    ],
    tests_require=[
        "pytest",
        "pytest-cov",
    ],
    setup_requires=[
        "pytest-runner",
    ],
    test_suite="music_streaming.tests",
)