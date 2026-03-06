from setuptools import find_packages, setup


setup(
    name="dlq-replay",
    version="0.1.0",
    packages=find_packages(include=["app", "app.*"]),
)
