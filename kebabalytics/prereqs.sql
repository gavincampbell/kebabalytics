
-- Run this to set up the schemas we use
CREATE CATALOG IF NOT EXISTS kebabalytics_dev;
CREATE CATALOG IF NOT EXISTS kebabalytics_prod;
CREATE CATALOG IF NOT EXISTS kebabalytics_shared;
CREATE SCHEMA IF NOT EXISTS kebabalytics_shared.raw;
CREATE VOLUME IF NOT EXISTS kebabalytics_shared.raw.sales;
