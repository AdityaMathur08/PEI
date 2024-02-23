
Running a DataBricks cluster on AWS - setup

Raw Dataset:

1. Orders.json

3.


Tasks:

Task

Create raw tables for each source dataset - 
- Will put the data in a landing zone, create external tables in spark. (Extract and Load)

Create an enriched table for customers and products
- this table needs to be created with a transformation script.
Create an enriched table which has
order information - we get it from Orders.json
Profit rounded to 2 decimal places
Customer name and country
Product category and sub category
Create an aggregate table that shows profit by
Year
Product Category
Product Sub Category
Customer
Using SQL output the following aggregates
Profit by Year
Profit by Year + Product Category
Profit by Customer
Profit by Customer + Year