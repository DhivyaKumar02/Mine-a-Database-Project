# 1. Library
library(RMySQL)
library(DBI)
library(RSQLite)
library(xml2)

# Define the file path for the SQLite database
db_file <- "my_database.db"

# Connect to the SQLite database
conn <- dbConnect(RSQLite::SQLite(), dbname = db_file)

###########################################

# Function to delete all tables
delete_tables <- function() {
  # Delete sales table
  dbExecute(conn, "DROP TABLE IF EXISTS sales")
  
  # Delete products table
  dbExecute(conn, "DROP TABLE IF EXISTS products")
  
  # Delete reps table
  dbExecute(conn, "DROP TABLE IF EXISTS reps")
  
  # Delete customers lookup table
  dbExecute(conn, "DROP TABLE IF EXISTS customers")
  
}

# Call the function to delete all tables
delete_tables()

###########################################

# Define the table creation queries separately
create_products_table_query <- "
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT
);
"

create_reps_table_query <- "
CREATE TABLE IF NOT EXISTS reps (
    rep_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    territory TEXT,
    commission FLOAT
);
"

create_customers_table_query <- "
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT,
    country TEXT
);
"

create_sales_table_query <- "
CREATE TABLE IF NOT EXISTS sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id INTEGER,
    product_id INTEGER,
    rep_id INTEGER,
    customer_id INTEGER,
    sale_date DATE,
    quantity INTEGER,
    total_amount FLOAT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (rep_id) REFERENCES reps(rep_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
"

# Execute each table creation query
dbSendQuery(conn, create_products_table_query)
dbSendQuery(conn, create_reps_table_query)
dbSendQuery(conn, create_customers_table_query)
dbSendQuery(conn, create_sales_table_query)


##############################################

extract_reps_data <- function(xml_file) {
  # Parse the XML document
  doc <- read_xml(xml_file)
  
  # Extract data from each <rep> element
  reps <- xml_find_all(doc, "//rep")
  
  # Initialize an empty list to store the extracted data
  rep_data <- list()
  
  # Loop through each <rep> element and extract data
  for (rep in reps) {
    rep_id <- xml_attr(rep, "rID")
    first_name <- xml_text(xml_find_first(rep, ".//first"))
    last_name <- xml_text(xml_find_first(rep, ".//sur"))
    territory <- xml_text(xml_find_first(rep, ".//territory"))
    commission <- as.numeric(xml_text(xml_find_first(rep, ".//commission")))
    
    # Add extracted data to the list
    rep_data[[rep_id]] <- c(rep_id, first_name, last_name, territory, commission)
  }
  
  # Convert the list to a matrix and transpose it
  rep_matrix <- t(sapply(rep_data, unlist))
  
  # Convert the matrix to a data frame
  rep_df <- as.data.frame(rep_matrix, stringsAsFactors = FALSE)
  
  # Assign column names
  colnames(rep_df) <- c("rep_id", "first_name", "last_name", "territory", "commission")
  
  return(rep_df)
}

##############################################

insert_reps <- function(connection, xml_file, on = TRUE) {
  # Extract reps data from the XML file
  reps_data <- extract_reps_data(xml_file)
  
  if (on) {
    # Start transaction
    dbBegin(connection)
  }
  
  # Batch size for insertion
  batch_size <- 1000  # Adjust as needed
  n <- nrow(reps_data)
  num_batches <- ceiling(n / batch_size)
  
  tryCatch({
    for (batch_index in 1:num_batches) {
      start <- (batch_index - 1) * batch_size + 1
      end <- min(batch_index * batch_size, n)
      
      batch_data <- reps_data[start:end, ]
      
      # Remove the leading 'r' character from rep_id values and convert to integer
      batch_data$rep_id <- as.integer(sub("r", "", batch_data$rep_id))
      
      # Construct parameterized SQL insert statement
      insert_statement <- "INSERT INTO reps (rep_id, first_name, last_name, territory, commission) VALUES "
      
      values <- paste0("('", batch_data$rep_id, "', '", batch_data$first_name, "', '", batch_data$last_name, "', '", batch_data$territory, "', ", batch_data$commission, ")")
      
      insert_statement <- paste0(insert_statement, paste(values, collapse = ", "), ";")
      
      # Execute batch insert statement
      dbExecute(connection, insert_statement)
    }
    
    if (on) {
      # Commit transaction
      dbCommit(connection)
      message("Data inserted successfully into the database.")
    }
  }, 
  error = function(e) {
    if (on) {
      # Rollback transaction in case of error
      dbRollback(connection)
    }
    stop("Error occurred during insertion: ", conditionMessage(e))
  })
}



######################################################


extract_products_data <- function(xml_file) {
  # Parse XML document
  doc <- read_xml(xml_file)
  
  # Find all <product> elements
  products <- xml_find_all(doc, "//product")
  
  # Extract product names
  product_names <- xml_text(products)
  
  # Return the extracted product names
  return(product_names)
}


################################################

insert_products <- function(connection, xml_file, on = TRUE) {
  # Extract products data from the XML file
  product_names <- unique(extract_products_data(xml_file))
  
  # Check if transactions should be used
  if (on) {
    # Start transaction
    dbBegin(connection)
  }
  
  tryCatch({
    # Batch size for insertion
    batch_size <- 1000  # Adjust as needed
    n <- length(product_names)
    num_batches <- ceiling(n / batch_size)
    
    for (batch_index in 1:num_batches) {
      start <- (batch_index - 1) * batch_size + 1
      end <- min(batch_index * batch_size, n)
      
      batch_data <- product_names[start:end]
      
      # Construct parameterized SQL insert statement
      insert_statement <- "INSERT INTO products (product_name) VALUES "
      values <- paste0("('", batch_data, "')")
      insert_statement <- paste0(insert_statement, paste(values, collapse = ", "), ";")
      
      # Execute batch insert statement
      dbExecute(connection, insert_statement)
    }
    
    # Commit transaction if using transactions
    if (on) {
      dbCommit(connection)
      message("Data inserted successfully into the database.")
    }
  }, 
  error = function(e) {
    # Rollback transaction in case of error if using transactions
    if (on) {
      dbRollback(connection)
    }
    stop("Error occurred during insertion: ", conditionMessage(e))
  })
}


#################################################

extract_customers_data <- function(xml_file) {
  # Parse XML document
  doc <- read_xml(xml_file)
  
  # Find all <customer> and <country> elements within <txn> elements
  customers <- xml_find_all(doc, "//txn/customer")
  countries <- xml_find_all(doc, "//txn/country")
  
  # Extract customer and country data
  customer_names <- xml_text(customers)
  country_names <- xml_text(countries)
  
  # Create a data frame with customer and country information
  customers_data <- data.frame(customer = customer_names, country = country_names, stringsAsFactors = FALSE)
  
  return(customers_data)
}

#################################################

insert_customers <- function(connection, xml_file, on = TRUE) {
  customers_data <- extract_customers_data(xml_file)
  
  if (on) {
    # Start transaction
    dbBegin(connection)
  }
  
  # Remove duplicates from the combination of customer_name and country
  customers_data <- customers_data
  
  # Batch size for insertion
  batch_size <- 1000  # Adjust as needed
  n <- nrow(customers_data)
  num_batches <- ceiling(n / batch_size)
  
  tryCatch({
    for (batch_index in 1:num_batches) {
      start <- (batch_index - 1) * batch_size + 1
      end <- min(batch_index * batch_size, n)
      
      batch_data <- customers_data[start:end, ]
      
      # Construct parameterized SQL insert statement
      insert_statement <- "INSERT INTO customers (customer_name, country) VALUES "
      
      values <- paste0("('", batch_data$customer, "', '", batch_data$country, "' )")
      
      insert_statement <- paste0(insert_statement, paste(values, collapse = ", "), ";")
      
      # Execute batch insert statement
      dbExecute(connection, insert_statement)
    }
    
    if (on) {
      # Commit transaction
      dbCommit(connection)
      message("Data inserted successfully into the database.")
    }
  }, 
  error = function(e) {
    if (on) {
      # Rollback transaction in case of error
      dbRollback(connection)
    }
    stop("Error occurred during insertion: ", conditionMessage(e))
  })
}


######################################################

extract_transactions_data <- function(xml_file, connection) {
  # Parse XML document
  doc <- read_xml(xml_file)
  
  # Find all <txn> elements
  txns <- xml_find_all(doc, "//txn")
  
  # Initialize lists to store data
  rep_ids <- list()
  txn_ids <- list()
  sale_dates <- list()
  quantities <- list()
  total_amounts <- list()
  customers <- character(0)
  countries <- character(0)
  products <- character(0)
  
  
  # Loop through each <txn> element and extract data
  for (txn in txns) {
    rep_id <- as.integer(xml_attr(txn, "repID"))
    txn_id <- xml_attr(txn, "txnID")
    date <- xml_text(xml_find_first(txn, ".//date"))
    qty <- as.integer(xml_text(xml_find_first(txn, ".//qty")))
    total <- as.numeric(xml_text(xml_find_first(txn, ".//total")))
    customer <- xml_text(xml_find_first(txn, ".//customer"))
    country <- xml_text(xml_find_first(txn, ".//country"))
    product <- xml_text(xml_find_first(txn, ".//product"))
    
    # Add extracted data to the respective lists
    rep_ids <- c(rep_ids, rep_id)
    txn_ids <- c(txn_ids, txn_id)
    sale_dates <- c(sale_dates, date)
    quantities <- c(quantities, qty)
    total_amounts <- c(total_amounts, total)
    customers <- c(customers, customer)
    countries <- c(countries, country)
    products <- c(products, product)
  }
  
  # Create a data frame with the extracted data
  transactions_data <- data.frame(
    repID = unlist(rep_ids),
    txnID = unlist(txn_ids),
    sale_date = unlist(sale_dates),
    quantity = unlist(quantities),
    total_amount = unlist(total_amounts),
    customer = customers,
    country = countries,
    product_id = products,
    stringsAsFactors = FALSE
  )
  
  # Replace "/" with "-"
  transactions_data$sale_date <- gsub("/", "-", transactions_data$sale_date)
  
  # Split the date strings by "-"
  date_parts <- strsplit(transactions_data$sale_date, "-")
  
  # Rearrange date parts (exchange year and month) with correct format
  modified_dates <- sapply(date_parts, function(x) {
    modified_date <- paste0(x[3], "-", sprintf("%02d", as.integer(x[2])), "-", sprintf("%02d", as.integer(x[1])))
    return(modified_date)
  })
  
  # Assign modified dates back to the date column
  transactions_data$sale_date <- modified_dates
  
  # Split the date strings by "-"
  date_parts <- strsplit(transactions_data$sale_date, "-")
  
  # Rearrange date parts (exchange day and month) with correct format
  modified_dates <- sapply(date_parts, function(x) {
    modified_date <- paste0(x[1], "-", x[3], "-", x[2])
    return(modified_date)
  })
  
  # Assign modified dates back to the date column
  transactions_data$sale_date <- modified_dates
  
  # Replace "NA-NA-NA" entries with NA
  transactions_data$sale_date <- ifelse(transactions_data$sale_date == "NA-NA-NA", NA, transactions_data$sale_date)
  
  # Query product ids from the products table
  product_ids <- dbGetQuery(connection, paste0("SELECT product_id, product_name FROM products"))
  
  # Map product_is to products table
  transactions_data$product_id <- product_ids[match(transactions_data$product_id, product_ids$product_name), "product_id"]
  
  # Execute SQL query to fetch customer_id, customer_name, and country from customer table
  customer_id <- dbGetQuery(connection, paste0("SELECT customer_id, customer_name, country FROM customers"))
  
  # Use match to find customer_id values based on matching customer and country
  matched_indices <- match(paste(transactions_data$customer, transactions_data$country),
                           paste(customer_id$customer_name, customer_id$country))
  
  # Add customer_id values to transactions_data dataframe based on matched indices
  transactions_data$customer_id <- customer_id$customer_id[matched_indices]
  
  # Convert customer_id to integer
  transactions_data$customer_id <- as.integer(transactions_data$customer_id)
  
  return(transactions_data)
}

#################################################

insert_sales <- function(connection, xml_file, on = TRUE) {
  # Extract sales data from the XML file
  sales_data <- extract_transactions_data(xml_file, connection)
  
  if (on) {
    # Start transaction
    dbBegin(connection)
  }
  
  # Batch size for insertion
  batch_size <- 1000  # Adjust as needed
  n <- nrow(sales_data)
  num_batches <- ceiling(n / batch_size)
  
  tryCatch({
    for (batch_index in 1:num_batches) {
      start <- (batch_index - 1) * batch_size + 1
      end <- min(batch_index * batch_size, n)
      
      batch_data <- sales_data[start:end, ]
      
      # Construct parameterized SQL insert statement
      insert_statement <- "INSERT INTO sales (txn_id, product_id, rep_id, customer_id, sale_date, quantity, total_amount) VALUES "
      
      # Prepare values for insertion
      values <- paste0("('", batch_data$txnID, "', '", batch_data$product_id, "', '", batch_data$repID, "', '", batch_data$customer_id, "', '", batch_data$sale_date, "', ", batch_data$quantity, ", ", batch_data$total_amount, ")")
      
      insert_statement <- paste0(insert_statement, paste(values, collapse = ", "), ";")
      
      # Execute batch insert statement
      dbExecute(connection, insert_statement)
    }
    
    if (on) {
      # Commit transaction
      dbCommit(connection)
      message("Data inserted successfully into the database.")
    }
  }, 
  error = function(e) {
    if (on) {
      # Rollback transaction in case of error
      dbRollback(connection)
    }
    stop("Error occurred during insertion: ", conditionMessage(e))
  })
}


#################################################

# Define path to the txn-xml folder
txn_xml_folder <- "txn-xml"

# Get list of XML files in the folder
xml_files <- list.files(path = txn_xml_folder, pattern = "\\.xml$", full.names = TRUE)

# Iterate over each XML file and process the data
for (xml_file in xml_files) {
  # Parse XML file
  doc <- read_xml(xml_file)
  
  # Check if the file contains sales reps data
  if (grepl("pharmaReps", xml_file)) {
    insert_reps(conn, xml_file  , TRUE)
  }else{
    insert_products(conn, xml_file, TRUE)
    insert_customers(conn, xml_file, TRUE)
  }
}
#################################################

# SQL query to remove duplicate entries from the products table
sql_query <- "
    DELETE FROM products
    WHERE product_id NOT IN (
        SELECT MIN(product_id)
        FROM products
        GROUP BY product_name
    )
"

# Execute the SQL query
dbExecute(conn, sql_query)

###################################################

# SQL query to remove duplicate entries from the customers table
sql_query <- "
    DELETE FROM customers
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM customers
        GROUP BY customer_name, country
    )
"

# Execute the SQL query
dbExecute(conn, sql_query)

##################################################

# Iterate over each XML file and process the data
for (xml_file in xml_files) {
  # Parse XML file
  doc <- read_xml(xml_file)
  
  if (grepl("pharmaSalesTxn", xml_file)) {
    insert_sales(conn, xml_file, TRUE)
  }
}

#################################################

# Close the database connection
dbDisconnect(conn)

#################################################


