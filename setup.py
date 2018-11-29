from setuptools import setup, find_packages

setup(
    name="mal_analytics",
    version="0.1.dev0",
    packages=find_packages(),
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    include_package_data=True,
    # data_files=[
    #     ('./data', ['mal_profiler/data/tables.sql',
    #                            'mal_profiler/data/add_constraints.sql',
    #                            'mal_profiler/data/drop_constraints.sql'])
    # ]
)
