# SQL Data Warehouse Project

This project demonstrates the construction of a modern Data Warehouse using SQL Server. It implements a medallion architecture (Bronze, Silver, Gold layers) to process, clean, and transform raw data into a structured format suitable for analytics.

![Data Architecture](Docs/Data_achitecture.drawio.png)

## Project Architecture

The data warehouse follows a three-layer architecture:

1.  **Bronze (Raw Layer):** Holds the raw data as-is from source systems (CRM and ERP).
2.  **Silver (Cleaned Layer):** Contains cleaned and standardized data. Transformations include handling nulls, trimming strings, data type conversions, and resolving duplicates.
3.  **Gold (Analytics Layer):** (In Progress/Planned) Fact and Dimension tables optimized for business intelligence and reporting.

## Directory Structure
```
├── Docs/
│   ├── Data_achitecture.drawio.png    # Architectural diagram
│   └── Data_integration.drawio.png    # Integration workflow
├── Data warehouse/
│   ├── datasets/                     # Source CSV files
│   │   ├── source_crm/               # CRM source data
│   │   └── source_erp/               # ERP source data
├── Scripts/
│   ├── init_database.sql             # Script to initialize DB and schemas
│   ├── bronze/                       # Bronze layer DDL and Load scripts
│   │   ├── ddl_bronze.sql            # Table definitions for Bronze
│   │   └── bronze_load_proc.sql      # Stored procedures for Bulk Loading
│   ├── silver/                       # Silver layer DDL and transformation scripts
│   │   ├── ddl_silver.sql            # Table definitions for Silver
│   │   ├── silver_load_proc.sql      # Stored procedures for ETL/Cleaning
│   │   └── dataCleaning_transformation_silver.sql
└── tests/                            # Validation and quality checks
```

## Getting Started

### Prerequisites

*   SQL Server (Express, Standard, or Developer edition)
*   SQL Server Management Studio (SSMS) or Azure Data Studio

### Setup Instructions

1.  **Initialize Database:**
    Run `Scripts/init_database.sql` to create the `DataWarehouse` database and the `bronze`, `silver`, and `gold` schemas.

2.  **Bronze Layer Setup:**
    *   Execute `Scripts/bronze/ddl_bronze.sql` to create the raw tables.
    *   Execute `Scripts/bronze/bronze_load_proc.sql` to create and run the stored procedure for loading data from CSVs.
    *   *Note: Update the file paths in the `BULK INSERT` statements to match your local environment.*

3.  **Silver Layer Setup:**
    *   Execute `Scripts/silver/ddl_silver.sql` to create the cleaned tables.
    *   Execute `Scripts/silver/silver_load_proc.sql` to perform data cleaning and transformation from Bronze to Silver.

## Key Features

*   **Medallion Architecture:** Logical separation of data based on quality and purpose.
*   **Stored Procedures:** Automated ETL processes for repeatable data loading.
*   **Data Cleaning:** Robust handling of marital status, gender codes, and date formats.
*   **Performance Optimization:** Use of `TABLOCK` and `TRUNCATE` for efficient bulk loading.

## Future Enhancements

*   Implementation of the Gold layer with Star Schema (Dimensions and Facts).
*   Integration with Power BI for data visualization.
*   Automated data quality testing scripts.
