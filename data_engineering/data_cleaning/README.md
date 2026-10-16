
# Data Cleaning

The **data_cleaning** module is designed to execute flows in the input layer. It orchestrates the execution of various data flows based on parameters provided by the user. It consists of multiple modules:

- **`./src`**: This directory contains submodules for each domain, such as `customer` and `credit_card`. Each submodule is responsible for cleaning processes specific to its dataset, saving data to storage in Delta format, and providing utility functions useful at the input layer.

- **`./customer`**: A submodule based on a main flow and tasks, specifically used for cleaning customer data.
- **`./credit_card`**: A submodule based on a main flow and tasks, specifically used for cleaning credit_card data.

# Configuration

The project uses YAML files to configure various aspects like data paths, environment settings, and the data catalog. The following YAML files are important:

- **`data_catalog.yml`**: Defines the sources and structure of data files.
- **`global_settings.yml`**: Contains global settings such as base paths and environment configurations.
- **`io_config.yml`**: Specifies input and output configurations for each data domain, ensuring that data is correctly routed and stored.
