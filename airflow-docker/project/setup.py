from setuptools import setup, find_packages

setup(
    name="quickshop_etl",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        # runtime deps can be empty here; the container also installs requirements.txt
    ],
)
