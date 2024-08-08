# 1. Library
library(RMySQL)
library(DBI)
library(RSQLite)

# 2. Settings
db_user <- 'dhivya'            #  value from the setup
db_password <- 'Practicum@2024'    #  value from the setup
db_name <- 'practicum12024'         #  value from the setup
db_host <- 'db4free.net'       # for db4free.net
db_port <- 3306

# 3. Connect to remote server database
con <-  dbConnect(RMySQL::MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

#############################################

# Define the file path for the SQLite database
db_file <- "my_database.db"

# Connect to the SQLite database
conn <- dbConnect(RSQLite::SQLite(), dbname = db_file)

#############################################

# Function to delete all tables
delete_tables <- function() {
  # Delete strikes table
  dbExecute(con, "DROP TABLE IF EXISTS  product_facts")
  
  # Delete strikes table
  dbExecute(con, "DROP TABLE IF EXISTS  rep_facts")
  
}

# Call the function to delete all tables
delete_tables()

##########################################

# Define the schema for the product_facts table with a specified key length for the TEXT column
create_product_facts_table_query <- "
CREATE TABLE IF NOT EXISTS product_facts (
    product_id INT,
    product_name TEXT,
    total_amount_sold FLOAT,
    quarter VARCHAR(10),
    sale_year VARCHAR(10),
    total_units_sold_per_region FLOAT,
    territory TEXT,
    PRIMARY KEY (product_id, sale_year, quarter, territory(255))
);
"
# Execute the query to create the product_facts table
dbSendQuery(con, create_product_facts_table_query)

create_rep_facts_table_query <- "
CREATE TABLE IF NOT EXISTS rep_facts (
    rep_name VARCHAR(255), 
    total_amount_sold FLOAT,
    average_amount_sold FLOAT,
    quarter VARCHAR(10),
    sale_year VARCHAR(10),
    PRIMARY KEY (rep_name, sale_year, quarter)
);
"

dbSendQuery(con, create_rep_facts_table_query)

############################################


# SQL query to select data
query <- "
SELECT 
    p.product_id,
    p.product_name,
    SUM(s.total_amount) AS total_amount_sold,
    CASE
        WHEN strftime('%m', s.sale_date) BETWEEN '01' AND '03' THEN 'Q1'
        WHEN strftime('%m', s.sale_date) BETWEEN '04' AND '06' THEN 'Q2'
        WHEN strftime('%m', s.sale_date) BETWEEN '07' AND '09' THEN 'Q3'
        WHEN strftime('%m', s.sale_date) BETWEEN '10' AND '12' THEN 'Q4'
        ELSE 'Unknown'
    END AS quarter,
    CAST(SUBSTR(s.sale_date, 1, 4) AS UNSIGNED) AS sale_year,
    SUM(s.quantity) AS total_units_sold_per_region,
    r.territory
FROM 
    products p
JOIN 
    sales s ON p.product_id = s.product_id
JOIN 
    reps r ON s.rep_id = r.rep_id
GROUP BY 
    p.product_id, sale_year, quarter, r.territory;
"

# Execute the query and fetch the results
result <- DBI::dbGetQuery(conn, query)

# Construct the INSERT statement
insert_statement <- paste0("INSERT INTO product_facts (product_id, product_name, total_amount_sold, quarter, sale_year, total_units_sold_per_region, territory) VALUES ",
                           paste("(", result$product_id, ", '", result$product_name, "', ", result$total_amount_sold, ", '", result$quarter, "', ", result$sale_year, ", ", result$total_units_sold_per_region, ", '", result$territory, "')", sep = "", collapse = ", "))


dbSendQuery(con, insert_statement)

###############################################

# Query to populate rep_facts table
populate_rep_facts_query <- "
SELECT
    CONCAT(r.first_name, ' ', r.last_name) AS rep_name,
    SUM(s.total_amount) AS total_amount_sold,
    AVG(s.total_amount) AS average_amount_sold,
    CASE
        WHEN strftime('%m', s.sale_date) BETWEEN '01' AND '03' THEN 'Q1'
        WHEN strftime('%m', s.sale_date) BETWEEN '04' AND '06' THEN 'Q2'
        WHEN strftime('%m', s.sale_date) BETWEEN '07' AND '09' THEN 'Q3'
        WHEN strftime('%m', s.sale_date) BETWEEN '10' AND '12' THEN 'Q4'
        ELSE 'Unknown'
    END AS quarter,
    CAST(SUBSTR(s.sale_date, 1, 4) AS UNSIGNED) AS sale_year
FROM
    reps r
JOIN
    sales s ON r.rep_id = s.rep_id
GROUP BY
    rep_name, quarter, sale_year;
"

# Execute the query to populate the rep_facts table
result <- dbGetQuery(conn, populate_rep_facts_query)

# Prepare the INSERT INTO statement
insert_statement <- sprintf("INSERT INTO rep_facts (rep_name, total_amount_sold, average_amount_sold, quarter, sale_year) VALUES %s;",
                            paste0("('", result$rep_name, "', ", result$total_amount_sold, ", ", result$average_amount_sold, ", '", result$quarter, "', ", result$sale_year, ")", collapse = ", "))

# Execute the INSERT INTO statement
dbExecute(con, insert_statement)


############################################################3
dbDisconnect(con)
dbDisconnect(conn)