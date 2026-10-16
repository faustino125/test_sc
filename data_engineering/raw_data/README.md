
# Raw Data

The **raw_data** module is designed to execute qa functionalities for all data sources. It orchestrates the execution of a data flow that can be based on parameters provided by the user or with a default behavior. It consists of a single module:

- **`./src`**: This directory contain a submodule called `missing_raw_data`. The submodule is responsible of visiting all data sources in the DL and getting all the missing dates for each data source, saving data to storage in Delta format as well as json format.

# Configuration

The project uses YAML files to configure various aspects like data paths, environment settings, and the data catalog. The following YAML files are important:

- **`data_catalog.yml`**: Defines the sources and structure of data files.
- **`io_config.yml`**: Specifies input and output configurations for each data domain, ensuring that data is correctly routed and stored.
