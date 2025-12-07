# 🎯 Goal

You've created your first staging models, which are refined versions of the `source` data inherited from the raw BigQuery dataset. This section has 2 goals:
- Build on top of your staging models to create an intermediate layer. We will create three intermediate models:
    - `int_sales_margin.sql`
    - `int_orders_margin.sql`
    - `int_orders_operational.sql`
- Build on top of your intermediate layers to create a mart layer, ready to serve to business stakeholders. We will create a single mart model:
    - `finance_days.sql`

❗ **First: copy `dbt_lewagon` from the previous challenge into this challenge** ❗

## Case study context reminder

<details>
<summary markdown="span"> Expand me!</summary>

This modules challenges follow a case study for an ELT pipeline. The dataset that you will be using is from the French company Greenweez. Greenweez is an organic e-commerce website that caters to B2C customers by offering a variety of products for a healthier and more sustainable lifestyle.

In these challenges you assume the role of a Data/Analytics Engineer working on a project with the finance team.

The finance team has requested the creation of a comprehensive table in BigQuery for conducting basket analysis. The table should encompass the following key metrics:

- **Daily Transaction Evolution:** Tracking the number of transactions (orders) that occur each day.
- **Daily Average Basket Evolution:** Monitoring the average basket amount for each day.
- **Daily Margin and Operational Margin Evolution**: Observing the evolution of both margin and operational margin on a daily basis.

In order to fulfill their requirements, they have specified the need for a well-structured data pipeline that adheres to the following principles:

#### Data Accuracy and Protection:

The pipeline should be designed to prevent the insertion of incorrect or erroneous data into the production environment, ensuring data accuracy and integrity.

#### Error Identification and Handling:

The pipeline should have mechanisms in place to break down complexity, making it easy to identify and handle any new errors that may arise without disrupting access to data.

#### Organized and Accessible Structure:

The mart layer data should be organized and stored in a separate dataset from the raw and intermediate data to prevent any potential misunderstandings regarding its structure or usage within the finance team.

#### Comprehensive Documentation:

Provide detailed and comprehensive documentation of the table, including its columns, and lineage to understand where the data are coming from to enhance understanding and usage.

</details>

<br>

# 1️⃣ Create Intermediate Models

Our goal is to create an intermediate layer that builds on the previously created **staging models**.

## 1.1. Margin per Product

The first intermediate model you will create is `int_sales_margin.sql`. It will log information about the purchase cost and margin made on **each product**.

Under the `models/intermediate/` directory, create a file called `int_sales_margin.sql`. Configure the model with the following requirements:
- Materialize the model as a **view** - think about why this model should be a view vs table.
- The output view should have the following fields:
    - `products_id`: product identifier
    - `orders_id`: order identifier
    - `date_date`: order date
    - `revenue`: price paid by customer to purchase the products
    - `quantity`: quantity of a product purchased in an order
    - `purchase_price`: Greenweez cost to obtain a single unit of the product
    - `purchase_cost`: Greenweez cost to obtain the products in an order
    - `margin`: Profit Greenweez makes on the sale of different products
- Round float values to two decimal places.
- Order by `orders_id` so that more recent orders appear first.

Metric definitions:
- `purchase_cost` = `quantity` * `purchase_price`
- `margin` = `revenue` - `purchase_cost`

💡 Suggested approach:
- Locate the data needed for the required fields and metrics in your different staging models and how to **reference** those models.
- Start with the `JOIN` clause and use `SELECT *` as a placeholder.
- Do not try to write SQL for all the fields at once. Start with one very simple query, creating 1 or 2 fields at a time. Then run your model and make sure the view is created in BigQuery, then iterate.

Make sure to run your model with the correct dbt command to create it in BigQuery!

🧪 When you think you have the correct model, run:

```bash
make test
```

1/4 tests should have passed!

## 1.2. Margin per Order

The second intermediate model you will create is `int_orders_margin.sql`. It will log information about the purchase cost and margin made on **each order**.

Under the `models/intermediate/` directory, create a file called `int_orders_margin.sql`. Configure the model with the following requirements:
- Materialize the model as a **view**
- The output view should have the following fields:
    - `orders_id`: order identifier
    - `date_date`: order date
    - `revenue`: price paid by customer to purchase all products in an order
    - `quantity`: quantity of products in an order
    - `purchase_cost`: Greenweez cost to obtain the products in an order
    - `margin`: profit Greenweez makes on the sale of products in an order
- Round float values to two decimal places.
- Order by `orders_id` so that more recent orders appear first.

💡 Hints:
- Think about what model(s) to reference. You are not constrained to referencing data from your staging models.
- Think about how you will manage the `date_date` column.

Make sure to run your model with the correct dbt command to create it in BigQuery!

🧪 When you think you have the correct model, run:

```bash
make test
```

2/4 tests should have passed!

## 1.3. Operational Margin per Order

The last intermediate model we will create is `int_orders_operational`. This model will capture information about the cost of shipping and logistics.

Under the `models/intermediate/` directory, create a file called `int_orders_operational.sql`. Configure the model with the following requirements:
- Materialize the model as a **view**
- The output view should have the following fields:
    - `orders_id`: order identifier
    - `date_date`: order date
    - `operational_margin`: profit Greenweez makes on the sale of products in an order after operational and logistics costs
    - `quantity`: quantity of products in an order
    - `revenue`: price paid by customer to purchase all products in an order
    - `purchase_cost`: Greenweez cost to obtain the products in an order
    - `margin`: profit Greenweez makes on the sale of products in an order, not accounting for operational and logistics costs
    - `shipping_fee`: fee customer pays for shipping an order
    - `log_cost`: Greenweez cost to prepare the parcel for delivery
    - `ship_cost`: Greenweez shipping cost paid to logistics provider to deliver order
- Round float values to two decimal places.
- Order by `orders_id` so that more recent orders appear first.

Metric definitions:
- `operational_margin` = ( `margin` + `shipping_fee` ) - ( `log_cost` + `ship_cost` )

💡 Hints:
- Think about what model(s) to reference. You are not constrained to referencing data from your staging models.

Make sure to run your model with the correct dbt command to create it in BigQuery!

🧪 When you think you have the correct model, run:

```bash
make test
```

3/4 tests should have passed!

## 1.4. View the Lineage

We've made a few models to transform our data. The lineage of your entire pipeline so far should look similar to the below in the [Power User for dbt](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user) extension in VS Code:

<img src="https://wagon-public-datasets.s3.amazonaws.com/data-engineering/030203-dbt-intermediate-lineage.png">

Alternatively, view the lineage on the Documentation web app that DBT can create for you:

To generate and serve docs you can use the following commands:

```bash
# Generate the documentation
dbt docs generate
```

```bash
# Serve the documentation in a web GUI
dbt docs serve
```

To view the the lineage graph, navigate to:

Database > your_project_name > your_dataset_name > `int_orders_operation` > click on the small blue button in the bottom right corner.

To view a larger, more detailed lineage, click on the **view fullscreen** button in the top right corner of the lineage preview.

<br>

# 2️⃣ Mart Model

Mart models create tables or views designed to serve other stakeholders in your business. In this case study, it will be the finance team. Because these other stakeholders may not be as technical as you, the goal is to have the majority of data available for the stakeholders to use with simple `SELECT` queries - within reason.

The finance team has sent across their data requirements, it's time to create a mart model that suits their needs.

## 2.1. Finance mart model

The mart model we will create is `finance_days`. This model will serve data for the finance team at a **daily granularity**.

Under the `models/mart/finance/` directory, create a file called `finance_days.sql`. Configure the model with the following requirements:
- Materialize the model as a **view** - Again, think if this model should materialize as a **view** or **table**. For now, materialize as a view.
- The output view should have the following fields:
    - `date_date`: date
    - `nb_transactions`: daily number of orders
    - `quantity`: daily quantity of all products sold
    - `revenue`: daily price paid by customers for all products sold
    - `average_basket`: daily average order revenue
    - `margin`: daily profit Greenweez makes on orders
    - `operational_margin`: daily profit Greenweez makes on an orders after operational and logistics costs
    - `purchase_cost`: daily Greenweez cost to obtain the products sold
    - `shipping_fee`: daily total price paid by all customers for shipping
    - `log_cost`: daily Greenweez cost to prepare parcels for delivery
    - `ship_cost`: daily Greenweez cost paid to logistics providers to deliver orders
- Round all float values to two decimal places.
- Order by `date_date` so that the data appears in reverse chronological order.

Make sure to run your model with the correct dbt command to create it in BigQuery!

🧪 When you think you have the correct model, run:

```bash
make test
```

4/4 tests should have passed!

## 2.2. Lineage

Feel free to have a look at the lineage in the Power User for dbt extension for VS Code or with DBT's docs (don't forget to generate and then serve), to see how your models relate to each other.

<br>

# 🏁 Finishing up

Congratulations! You've created a pipeline from a raw source dataset: **raw_gz_data**, and created a series of data models to clean, process, and transform the data to a table that is ready to be consumed by the finance team!

At the moment we have manually defined all our models materialisation as either a **table** or **view** in each model. Defining materialization this way can be tedious and prone to error. We also have the `finance_days` model materialising in the same BigQuery dataset as our **staging** and **intermediate** models. That doesn't sound secure 🤔

In the next challenge we'll address testing, documentation, default materialization, and how to materialize models into separate datasets! 🚀

🧪 To test all of your code, run:

```bash
# Be sure to run make commands from the challenge root folder!
make test
```

Don't forget to git add, commit, and push your code to github so you can track your progress on Kitt!

<br>
