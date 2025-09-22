This file helps in familiarizing with DAGSTER

## A. Test Run 1 (Hello, World!)

1. Create a folder ```dbt-dagster``` 
2. Create a python virtual environment ```(.venv)``` using ```python -m venv .venv```
3. Create a ```requirments.txt``` file and add
   ```
   dagster
   dagster-webserver
   polars
   duckdb
   ```
4. Install all the dependencies by running the following command ```pip install -r requirements.txt```
5. Create ```assets.py``` file that contains the asset initialization/transformation function calls and definitions
6. Run ```dagster dev -m assets```, which will locally launch Dagster services at http://127.0.0.1:3000
7. Explore the UI once it is up and running!

# B. Test Run 2 (Car Data)

1. Use the following command to scaffold a dagster project
    ```
    dagster project scaffold --name car_data
    ```
   This will create a directory with all required empty files, like:
   ```
   car_data/
   ├── car_data/                     # Your main Python package
   │   ├── __init__.py
   │   ├── assets.py                 # Example asset
   │   ├── definitions.py
   │
   ├── car_data_tests/               # Starter test suite
   │   ├── __init__.py
   │   └── test_assets.py
   │
   ├── pyproject.toml                # Project metadata & dependencies
   ├── setup.cfg                     # Config for linters/formatters
   ├── workspace.yaml                # Workspace entrypoint for Dagster
   └── README.md                     # Project README
   ```
2. Update the ```car_data/car_data/assets.py``` to add methods
3. Run ```dagster dev``` to start the local webserver
4. Materialize the assets and to check the content of the duckdb:
   1. Create a new bash terminal
   2. Activate the virtual environment
   3. Run ```python```, then
   
      1.```import duckdb```
   
      2.```conn=duckdb.connect(database="data/car_data.duckdb")```
   
      3.```conn.execute("select * from avg_price_per_brand").fetchall()```
   
   The following will be the output:
   ```
   [('isuzu', 8916.5), ('mercedes-benz', 33647.0), ('mitsubishi', 9239.76923076923), ('audi', 17859.166666666668), ('mazda', 9924.538461538461), ('subaru', 8541.25), ('mercury', 16503.0), ('nissan', 10415.666666666666), ('jaguar', 34600.0), ('volkswagen', 10077.5), ('alfa-romero', 15498.333333333334), ('saab', 15223.333333333334), ('volvo', 18063.18181818182), ('plymouth', 7963.428571428572), ('toyota', 9885.8125), ('bmw', 26118.75), ('dodge', 7875.444444444444), ('chevrolet', 6007.0), ('honda', 8184.692307692308), ('porsche', 31400.5), ('peugot', 15489.09090909091)]

   ```