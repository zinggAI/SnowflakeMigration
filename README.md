# SnowflakeMigration
Moving data stored in your stage from one environment to another and storing them locally in the process

# Prerequisites

- Python 3.9 or later (Python 3.8 is also supported)
- Snowflake organization administrator approval
- (If using Jupyter Notebooks) Jupyter Notebook installed
- (If using an IDE) Visual Studio Code with the Python extension
- Recommended to use command prompt terminal in VScode

# Installation

1. Create a Python virtual environment using conda or another tool:

   conda create --name py39_env --override-channels -c https://repo.anaconda.com/pkgs/snowflake python=3.9 numpy pandas

2. Activate the virtual environment:

   conda activate py39_env

3. Install the required packages:

   pip install -r requirements.txt

-- If any package not installed correctly, install manually inside environment using `pip install <package-name>`

# Steps

1. Update source environment and destination environment details in SnowflakeConnSource.properties and SnowflakeConnDest.properties
2. Input details such as name of the project, data source, name of the data source.
3. run `python copy_data.py` to copy data from one Snowflake environment to another.

# References:

1. Snowflake setup documentation - https://docs.snowflake.com/en/developer-guide/snowpark/python/setup
2. Snowflake#pandas installation - https://docs.snowflake.com/developer-guide/python-connector/python-connector-pandas#installation 
3. Snowflake Extension for VS Code setup - https://docs.snowflake.com/en/user-guide/vscode-ext
4. Moving data between Snowflake environments - https://medium.com/qonto-way/how-to-move-data-between-your-snowflake-environments-in-under-5-minutes-9c37ed59d091
