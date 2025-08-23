-- Enhanced streaming database schema for realistic retail events

-- Drop existing table if it exists
DROP TABLE IF EXISTS stream_sales_events;

-- Enhanced sales events table
CREATE TABLE stream_sales_events (
    event_id VARCHAR(36) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    store_id INT NOT NULL,
    dept_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount > 0),
    transaction_type ENUM('SALE', 'RETURN', 'EXCHANGE') NOT NULL DEFAULT 'SALE',
    payment_method ENUM('CASH', 'CARD', 'MOBILE') NOT NULL DEFAULT 'CARD',
    promotion_applied BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for common queries
    INDEX idx_store_time (store_id, event_time),
    INDEX idx_dept_time (dept_id, event_time),
    INDEX idx_event_time (event_time),
    INDEX idx_customer (customer_id),
    INDEX idx_transaction_type (transaction_type)
);

-- Real-time inventory tracking table
CREATE TABLE stream_inventory_events (
    event_id VARCHAR(36) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    store_id INT NOT NULL,
    dept_id INT NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    event_type ENUM('SALE', 'RESTOCK', 'WASTE', 'TRANSFER') NOT NULL,
    quantity_change INT NOT NULL,
    current_stock INT NOT NULL CHECK (current_stock >= 0),
    reorder_point INT DEFAULT 10,
    supplier_id VARCHAR(20),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for inventory queries
    INDEX idx_product_stock (product_id, current_stock),
    INDEX idx_store_product (store_id, product_id),
    INDEX idx_low_stock (store_id, current_stock, reorder_point)
);

-- Customer behavior tracking table
CREATE TABLE stream_customer_events (
    event_id VARCHAR(36) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    store_id INT NOT NULL,
    dept_id INT,
    event_type ENUM('ENTRY', 'DEPT_VISIT', 'PRODUCT_VIEW', 'CART_ADD', 'CART_REMOVE', 'CHECKOUT', 'EXIT') NOT NULL,
    product_id VARCHAR(50),
    session_id VARCHAR(36) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for customer journey analysis
    INDEX idx_customer_session (customer_id, session_id, event_time),
    INDEX idx_store_customer (store_id, customer_id),
    INDEX idx_session_journey (session_id, event_time)
);

-- Real-time aggregation tables for fast queries

-- Hourly sales summary
CREATE TABLE rt_hourly_sales (
    store_id INT,
    dept_id INT,
    sale_date DATE,
    sale_hour TINYINT,
    transaction_count INT DEFAULT 0,
    total_sales DECIMAL(12,2) DEFAULT 0,
    avg_transaction_value DECIMAL(10,2) DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (store_id, dept_id, sale_date, sale_hour),
    INDEX idx_date_hour (sale_date, sale_hour)
);

-- Current inventory status
CREATE TABLE rt_inventory_status (
    store_id INT,
    dept_id INT,
    product_id VARCHAR(50),
    current_stock INT NOT NULL,
    reorder_point INT DEFAULT 10,
    stock_status ENUM('NORMAL', 'LOW_STOCK', 'OUT_OF_STOCK') GENERATED ALWAYS AS (
        CASE 
            WHEN current_stock <= 0 THEN 'OUT_OF_STOCK'
            WHEN current_stock <= reorder_point THEN 'LOW_STOCK'
            ELSE 'NORMAL'
        END
    ) STORED,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    PRIMARY KEY (store_id, product_id),
    INDEX idx_stock_status (stock_status),
    INDEX idx_low_stock_alert (store_id, stock_status, last_updated)
);

-- Performance monitoring table
CREATE TABLE streaming_metrics (
    metric_time TIMESTAMP NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value DECIMAL(15,4),
    store_id INT,
    dept_id INT,
    
    INDEX idx_metric_time (metric_name, metric_time),
    INDEX idx_store_metrics (store_id, metric_time)
);