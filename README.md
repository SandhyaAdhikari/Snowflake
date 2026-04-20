# Snowflake...

Introduction to Data Warehouses & Workspaces
 
What is a Data Warehouse?

A data warehouse is a system used to store large amounts of structured data for analysis and reporting.

* It collects data from different sources
* Optimized for querying (not transactions)
* Used in business intelligence and analytics


Snowflake Overview

Snowflake is a cloud-based data warehouse platform.

Key features:

* Fully managed (no infrastructure setup)
* Scalable compute and storage
* Supports SQL for querying data

---

Snowflake Architecture (Basic Idea)

Snowflake separates:

1. **Storage** → where data is stored
2. **Compute (Virtual Warehouses)** → where queries run
3. **Cloud Services** → manages everything

---

What is a Virtual Warehouse?

A virtual warehouse in Snowflake is a compute engine used to:

* Run SQL queries
* Load data
* Perform transformations

You can start, stop, and scale warehouses independently.

---

What is a Workspace?

In Snowflake, your workspace includes:

* Databases
* Schemas
* Tables
* Worksheets (where you write SQL)

---
Key Concepts

* Data warehouse = centralized analytics storage
* Snowflake = cloud data warehouse
* Virtual warehouse = compute power
* Workspace = environment where you interact with data
