# 📊 Weekly_Report__Sales_Pipeline

## Overview
This repository contains a **Python-based data processing pipeline** used to generate a **clean, enriched, and standardized weekly sales report** from multiple raw CSV sources.

The script consolidates **debit notes, agents, relationship officers (ROs), branches, regions, and channels**, applies business rules, and outputs a **final analytics-ready report**.

---

## 🚀 Key Objectives
- Merge weekly sales data from multiple sources
- Standardize **RO names**, **agent channels**, **branches**, and **regions**
- Automatically classify **Specialty**, **Channel Type**, and **Transaction Type**
- Ensure consistent column structure for downstream reporting
- Remove duplicates and handle missing values intelligently

---

## 📂 Input Files
The script expects the following CSV files in the project directory:

| File Name | Description |
|----------|------------|
| `Weekly_report_1.csv` | Main weekly sales data |
| `Weekly_Report_RO_1.csv` | Debit note to RO mapping |
| `Agents_RM.csv` | Agent-to-RM mapping |
| `rm_branch_region.csv` | RO to Branch and Region mapping |
| `Agents_channel.csv` | Agent to Channel mapping |

---

## 📄 Output Files

| File Name | Description |
|----------|------------|
| `Weekly_report_40_check5.csv` | Intermediate merged dataset |
| `Weekly_report_1_check_final.csv` | **Final cleaned and standardized weekly report** |

---

## 🔧 Major Processing Steps

### 1. Data Loading & Standardization
- Reads all CSV files using `pandas`
- Cleans column names and standardizes text fields
- Renames columns for consistency

---

### 2. RO, Branch & Region Enhancements
- Adds new Relationship Officers (ROs) dynamically
- Normalizes multiple name variations (e.g., Ikeja RO names)
- Maps **Branch** and **Region** using reference tables
- Falls back to agent-based RM mapping when RO is missing

---

### 3. Agent & Channel Updates
- Appends new agents and assigns correct channels
- Applies manual channel corrections using business rules
- Ensures agent names are standardized before mapping

---

### 4. Deduplication & Data Integrity
- Removes duplicates based on:
  - Debit Note Number
  - Agent
  - RO Name
  - Branch
- Ensures one-to-one mapping where required

---

### 5. Business Rule Enrichment

#### Specialty Classification
