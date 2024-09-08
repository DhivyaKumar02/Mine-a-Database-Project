# Practicum II CS5200 Report

## Author
Dhivya Kumaraguruparan

## Date
Spring 2024

## Overview
This repository contains the R Markdown report for Practicum II CS5200. The report includes various analytical queries and visualizations based on the sales data.

## Table of Contents
1. [Project Description](#project-description)
2. [Analytical Queries](#analytical-queries)
3. [How to Run](#how-to-run)
4. [Dependencies](#dependencies)
5. [Contact](#contact)

## Project Description
This project involves analyzing sales data to gain insights into sales performance. The R Markdown report includes the following analytical queries:
- Top five sales reps with the most sales broken down by year
- Total sold per product per quarter
- Number of units sold per product per region
- Average sales per sales rep over the years

## Analytical Queries
1. **Top five sales reps with the most sales broken down by year**: Identifies the top sales representatives for each year based on their total sales.
2. **Total sold per product per quarter**: Provides insights into the sales performance of products throughout the quarters of each year.
3. **Number of units sold per product per region**: Shows the quantity of units sold for each product across different regions.
4. **Average sales per sales rep over the years**: Analyzes the average sales amount per sales representative over multiple years.

## How to Run
To generate the report, follow these steps:

1. **Install Dependencies**: Ensure that you have R and the required R packages installed. The necessary packages are `RMySQL`, `DBI`, `RSQLite`, `ggplot2`, `plotly`, and `knitr`.

2. **Setup Database Connection**: Update the database connection settings in the R Markdown file with your own credentials.

3. **Run the R Markdown File**: Open the R Markdown file in RStudio or any other R environment and knit the document to HTML or PDF.

## Dependencies
The following R packages are required:
- `RMySQL`
- `DBI`
- `RSQLite`
- `ggplot2`
- `plotly`
- `knitr`

You can install these packages using the following R commands:

```r
install.packages(c("RMySQL", "DBI", "RSQLite", "ggplot2", "plotly", "knitr"))
```