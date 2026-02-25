# Data Catalog: Gold Layer

## Gold Layer
The **Gold Layer** is the final layer of the Medallion Architecture. It represents the presentation-ready data, structured in a way that is optimized for business intelligence, reporting, and analytics. 

Key characteristics of the Gold Layer include:
- **Business-Level Aggregations:** Data is often aggregated or transformed into dimensions and facts.
- **High Quality:** Data has undergone cleaning (Bronze) and standardization/validation (Silver).
- **User-Centric:** Tables are designed to be easily understood by business users and analysts.
- **Consistency:** Provides a "Single Version of Truth" for the entire organization.

---

## 1. `gold.dim_customers`
**Description:** This table contains consolidated and cleaned customer information, merged from CRM and ERP systems.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `cst_id` | INT | Unique identifier for the customer (CRM system). |
| `cst_key` | NVARCHAR | Unique business key for the customer. |
| `cst_firstname` | NVARCHAR | Customer's first name. |
| `cst_lastname` | NVARCHAR | Customer's last name. |
| `cst_marital_status` | NVARCHAR | Customer's marital status (e.g., Single, Married). |
| `cst_gndr` | NVARCHAR | Customer's gender. |
| `cst_create_date` | DATE | Date the customer record was created in the CRM system. |
| `bdate` | DATE | Customer's birth date (from ERP system). |
| `gen` | NVARCHAR | Gender information (from ERP system). |
| `cntry` | NVARCHAR | Country of residence (from ERP system). |

---

## 2. `gold.dim_products`
**Description:** This table contains cleaned and structured product information from the CRM system. It implements **Slowly Changing Dimension (SCD) Type 2** logic to maintain historical changes.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `prd_id` | INT | Unique identifier for the product. |
| `cat_id` | NVARCHAR | Category identifier for the product. |
| `prd_key` | NVARCHAR | Unique business key for the product. |
| `prd_name` | NVARCHAR | Name of the product. |
| `prd_cost` | INT | Cost of the product. |
| `prd_line` | NVARCHAR | Product line (e.g., Mountain, Road). |
| `prd_start_date` | DATETIME | Start date of the product version (SCD Type 2). |
| `prd_end_date` | DATETIME | End date of the product version (NULL if current version). |

---
