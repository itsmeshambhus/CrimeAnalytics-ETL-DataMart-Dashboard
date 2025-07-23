# 🚓 Police Crime Data Mart – Advanced Databases Project

This project was developed as part of the **Advanced Databases** module, focusing on designing and implementing a complete **Data Mart (DM)** system using real-world techniques. The case study centers on analyzing police crime data to support decision-making and strategic reporting using business intelligence tools.

---

## 🎯 Chosen KPI: KPI 3 – Identify Areas with Crime Hotspots

The project aims to support the Police Force in identifying and analyzing **regional crime patterns**, helping decision-makers understand **which areas have the most reported crimes** over time.

---

## ⚙️ Project Workflow & Architecture

### 🔹 1. Star Schema Design

- **Version 1**: Designed an ideal Star Schema using QSEE to support broad analytical needs.
- **Version 2**: Adapted based on available data sources (PRCS, PS-Wales).
- Schema includes:
  - Fact Table: `crime_fact`
  - Dimension Tables: `station_dim`, `crime_type_dim`, `time_dim`, `status_dim`, etc.
  - SCD (Slowly Changing Dimensions) applied to dimension updates.

### 🔹 2. ETL Pipeline (Advanced Workflow)
- **Staging Tables** created to temporarily hold raw data.
- **Data Quality Separation**:
  - Filtered into **Good Data** and **Bad Data** tables.
  - Cleaned and corrected bad data into valid format.
- **ETL Execution**:
  - Populated fact and dimension tables using cleaned data.
  - Used **triggers**, **sequences**, and **functions** for automation and ID generation.
  - ETL included:
    - Data validation
    - Transformation
    - Business rules
- ETL implemented using **PL/SQL procedures**, **scripts**, and Oracle SQL.

### 🔹 3. Advanced Database Features
- ✅ **Partitioning**:
  - **Horizontal**: Separated data by crime year.
  - **Vertical**: Split tables into frequently used and archival attributes.
- ✅ **PL/SQL Package**:
  - Includes one **function**, one **procedure**, and one **package**.
- ✅ **Fact Table Population**:
  - Fully populated with consistent measures (crime count, escalation flag, etc.)
- ✅ **View Table Creation**:
  - Created a **reporting view** from the fact and dimension tables.
  - Used for dashboard/report visualizations across tools.

---

## 📊 Reports & Dashboards

A total of **15 reports** were built using:
- **Oracle APEX** (5 reports)
- **Excel Pivot Tables** (5 reports)
- **Tableau Dashboards** (5 reports)

### 🔸 Oracle APEX Reports
- Crime Distribution by Station/Region
- Top Crime Location Ranking
- Crime Severity by Station/Region
- Crime Type Concentration by Area
- Crime Frequency by Station and Time Period
  

### 🔸 Excel Pivot Reports
- Total Station by Resolved Crime
- Crime type by region
- Most Crime in Month
- Total Crime by City
- Repeated offence by station

### 🔸 Tableau Dashboards
- Police Station Comparison by Crime Types
- Crime Resolution Efficiency by Region
- Crime Trend Analysis
- Crime Trends Over Time
- Police Station Performance

> 📁 Screenshots available in `/reportViz/` folders by tool.

---

