import re
## orders:
orders_external_table_schema = """`Row ID` int,
                                   `Order Id` string,
                                   `Order Date` string,
                                    `Ship Date` string,
                                    `Ship Mode` string,
                                    `Customer ID` string,
                                    `Product ID` string,
                                    Quantity int,
                                    Price float,
                                    Discount float,
                                    Profit float"""

external_order_s3_bucket_path = "s3a://pei-bronze/orders_extract.csv"

## Product:

product_external_table_schema = """ `Product ID` string,
                                    `Category` string,
                                    `Sub-Category` string,
                                    `Product Name` string,
                                    `State` string,
                                    `Price Per Product` float
                                    """

external_product_s3_bucket_path = "s3a://pei-bronze/product_extract.csv"

## Customer:

customer_external_table_schema = """ `Customer ID` string,
                                    `Customer Name` string,
                                    `Email` string,
                                    `Phone` string,
                                    `Address` string,
                                    `Segment` string,
                                    `Country` string,
                                    `City` string,
                                    `State` string,
                                    `Postal Code` string,
                                    `Region` string
                                    """

external_customer_s3_bucket_path = "s3a://pei-bronze/customers_extract.csv"

def clean_string(s):
    if s is None:
        return ''
    else:
        return re.sub(r'[,\n\t]', ' ', s)

