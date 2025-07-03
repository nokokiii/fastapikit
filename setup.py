from setuptools import setup, find_packages

setup(
    name="fastapi-utils",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "surrealdb"
    ],
)