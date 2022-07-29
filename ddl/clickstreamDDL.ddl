CREATE TABLE clickStream (id INT,
                         event_timestamp TIMESTAMP,
                         device_type STRING,
                         session_id STRING,
                         visitor_id STRING,
                         item_id STRING,
                         redirection_source STRING,
                         is_add_to_cart BOOLEAN,
                         is_order_placed BOOLEAN);
