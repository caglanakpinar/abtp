import setuptools
from setuptools import find_packages


with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="abtest",
    version="0.0.4",
    author="Caglan Akpinar",
    author_email="cakpinar23@gmail.com",
    description="allows to run AB Test for any problem, automatically decides which test must be applied and represents the results",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords='A-B Test Control - Active Group Test AB Test via Bayesian Approach',
    packages= find_packages(exclude='__pycache__'),
    py_modules=['ab_test_platform/docs', 'ab_test_platform'],
    install_requires=[
                      "numpy >= 1.18.1",
                      "pandas >= 0.25.3",
                      "scipy >= 1.4.1 ",
                      "schedule >= 0.6.0",
                      "PyYAML",
                      "psycopg2-binary",
                      # "psycopg2 >= 2.8.5",
                      "python-dateutil >= 2.8.1",
                      "google-cloud-bigquery",
                      "mysql-connector-python",
                      "plotly >=  4.5.0",
                      "dash-html-components >= 1.0.2",
                      "dash-core-components >=  1.8.0",
                      "dash >= 1.9.0"
    ],
    url="https://github.com/caglanakpinar/abtp",
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3',
)