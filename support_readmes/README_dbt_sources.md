# 🎯 Goals and Context

This modules challenges follow on from each other. Piece by piece, you will build a robust DBT project based on a case study. As an overview, here is what we'll be doing in each challenge:
- **Setup DBT** - you've built the initial structure and had a brief, hands on introduction to DBT.
- **DBT Sources** - You'll define sources, the raw data your pipeline is reading from, and create staging models, that read from your sources and perform quality check.
- **DBT Modelling** - You'll create intermediate and mart layer models, performing aggregations and transformations to create the dataset to serve your end stakeholders
- **DBT Tests and Materialization** - Create configuration files for tests and documentations for every layer of your pipeline, as well as controlling which BigQuery dataset your mart models are materialized for managing access.
- **DBT Jinja Macros** - Create functions in Jinja to keep your SQL code DRY.
- **Recap** - See how to configure different targets to move your pipeline between development and production.

In this challenge you'll prepare your raw data sources and create staging models, which are refined versions of the raw data that you've been provided with. We'll also apply some quality checks to our data.

## Case Study Context

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

The DBT pipeline should effectively cater to these requirements, ensuring the finance team has access to up-to-date and accurate information for their daily dashboard needs.

## A reminder of the data

A reminder on the tables in the raw, source data that we'll be using in the next few challenges:
- **raw_gz_sales** - Timestamped sales with order_id, product_id, revenue, quantity
- **raw_gz_product** - product_id, purchSE_PRICE
- **raw_gz_ship** - Logistics data with fees and costs

<details>
<summary markdown='span'>💡 Expand for more info on the data</summary>
See below for the first three rows of the data in each table and a description of each field.

---

Sales Table

| date_date  | orders_id | pdt_id | revenue | quantity |
|------------|-----------|--------|---------|----------|
| 2021-04-01 | 825408    | 29372  | 1.92    | 2        |
| 2021-04-01 | 825408    | 119246 | 12.07   | 1        |
| 2021-04-01 | 825409    | 88694  | 2.94    | 1        |

Fields:
- `date_date` - order date
- `orders_id` - order identifier - foreign key to ship table
- `pdt_id` - product identifier - foreign key to product table
- `revenue` - price paid by customer to purchase the products
- `quantity` - quantity of a product purchased in an order

---

Product Table

| products_id | purchSE_PRICE |
|-------------|---------------|
| 95325       | 0             |
| 5547        | 2             |
| 5652        | 2             |

Fields:
- `products_id` - product identifier - primary key
- `purchSE_PRICE` - cost for Greenweez to obtain a single unit of the product

---

Ship Table

| orders_id | shipping_fee | shipping_fee_1 | logCost | ship_cost |
|-----------|--------------|----------------|---------|-----------|
| 1002259   | 3.54         | 3.54           | 8.0     | 2         |
| 1001464   | 20.0         | 20.0           | 10.25   | 2         |
| 997252    | 0.82         | 0.82           | 12.05   | 2         |

Fields:
- `orders_id` - order identifier - primary key
- `shipping_fee` - fee customer pays for a shipping an order
- `shipping_fee_1` - fee customer pays for shipping an order
- `logCost` - Greenweez cost to preparing the parcel for delivery
- `ship_cost` - Greenweez shipping cost paid to logistics provider to deliver order

</details>

## 🧪 A note on tests

The tests for this unit are testing against real BigQuery tables and views that your DBT models create in each challenge, or by checking the file structure of `0x-xxx/dbt_lewagon` folder. Make sure that your solutions are always in the `dbt_lewagon` project of the current challenge.

As always, feel free to have a look in the `tests/` directory and see how the tests are working!

<br>

# 0️⃣ Setup

We'll be using the same DBT project for each challenge, so lets start by copying the `dbt_lewagon` directory from the previous challenge, `01-Setup-DBT`, to the current challenge root.

<br>

# 1️⃣ Configuring Sources

We have our raw data loaded and our DBT project initialized, let's start with configuring the data sources, where DBT is retrieving our data, and what our models will work with.

- Under the `models/source` folder, create a file named `sources.yml`. This is where you'll configure the reference to the source BigQuery dataset.
- Populate this file so that DBT understands:
    - Which BigQuery dataset the source data is in
    - Which tables in this dataset we are reading from
    - Have a look at the source properties documentation at [this link](https://docs.getdbt.com/reference/source-properties) for more details on how to configure a source in DBT.
- Configure 1 source dataset:
    - `raw_gz_data`
- Configure 3 source tables:
    - `raw_gz_sales`
    - `raw_gz_product`
    - `raw_gz_ship`
- Optionally - Alias the source dataset and three source tables for easier referencing within DBT models:
    - `raw_gz_data` -> `raw`
    - `raw_gz_sales` -> `sales`
    - `raw_gz_product` -> `product`
    - `raw_gz_ship` -> `ship`

<details>
<summary markdown="span">💡 Hint</summary>

How to define sources:

```yml
# dbt_lewagon/models/source/sources.yml

version: 2

sources:
  - name: ...
    schema: ...
    tables:
      - name: ...
        identifier: ...
      - name: ...
        identifier: ...
```

Remember that a `schema` is a BigQuery dataset in DBT!

For aliasing, try to understand the difference between `name` vs `schema` and `name` vs `identifier`.
</details>

<br>

# 2️⃣ Configuring your first staging model

Let's start with configuring your first **staging model** that reads from `gz_raw_sales` in your `gz_raw_data` dataset.

- Under the `models/staging/` folder, create a file called `stg_raw_sales.sql`.
- Configure the model with the following:
    - It should be materialized as a **view**
    - It should be reading from the `<your_gcp_project_id>.raw_gz_data.raw_gz_sales` table. Remember to use the correct source syntax.
    - Keep all the columns
    - Rename the column: `pdt_id` to `products_id`

When you think you have a working model, run the model by executing the following command in your terminal:

```bash
# functionally identical commands:
dbt run -m stg_raw_sales
# OR
dbt run --select stg_raw_sales
```
And check that the table `stg_raw_sales` has been created in your BigQuery dataset.

🧪 To test your your models, run:

```bash
make test_stg_raw_sales
```

<br>

# 3️⃣ Configure One Model per Source

Now that you have a working model for your `sales` source. Generate two additional **staging models** for both the `product` and `ship` sources with the following requirements:

Product:
- Name the model: stg_raw_product.sql
- Materialize as a **view**
- Rename `purCHASE_PRICE` to `purchase_price` and cast to `FLOAT64`
- Keep all the columns

Ship:
- Name the model: stg_raw_ship.sql
- Materialize as a **view**
- Check the difference between `shipping_fee` and `shipping_fee_1`. You can use the BigQuery console to execute exploration queries. Use a `WHERE` clause and an inequality operator to determine the difference between the two columns.
- Rename `logCost` to `log_cost`
- Cast `ship_cost` to an appropriate data type.

When you think you have your models running, execute them individually with:

```bash
dbt run -m stg_raw_product
dbt run -m stg_raw_ship
```

Or run all your models with:

```bash
dbt run
```

If everything runs with no problems, have a look on BigQuery to check that the tables have been created! If something has gone wrong, you can safely delete the `dbt_<your_name>_day1` dataset on BigQuery and execute another `dbt run` to recreate the dataset and tables.

Here's a graphical representation of the sources and staging models:

<img src="https://wagon-public-datasets.s3.amazonaws.com/data-engineering/0302-dbt-sources-staging.png">

Check out the Power User for dbt extension for VS Code [at this link](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user). It has some useful utilities, like viewing lineage graphs in your code editor!

<br>

# 4️⃣ Adding Documentation and Tests

Documentation and tests are integral for your pipeline to be robust and for your team members (and yourself!) to understand how the project and models work.

While not the most glamorous part of engineering, documentation and tests are some of the most essential parts of writing good software that is easy to maintain. Luckily DBT makes it *relatively* easy to add tests and documentation to a DBT project.

## 4.1. Source Documentation

DBT allows users to add documentation as code in each `.yml` file. This makes it easy to add documentation as you progress through a project. By adding documentation as code, DBT can then source our `.yml` files to serve those docs in an easy to consume way.

Let's start by adding some documentation to our sources.

❓ Add a description about the schema (BigQuery dataset), **every table**, and **every column** in the `sources.yml` file.

<details>
<summary markdown="span">💡 Hint</summary>
Here is an example for the first column of `raw_gz_sales`

```yml
# sources.yml

version: 2

sources:
  - name: raw
    schema: raw_gz_data
    tables:
      - name: sales
        identifier: raw_gz_sales
        description: sales on Greenweez. One row per product_id found in each orders_id
        columns:
          - name: date_date
            description: order date
```

Make sure you fill out the rest!

Look around this challenge, you might find column descriptions somewhere 😉
</details>

To generate and serve docs you can use the following commands:

```bash
# Generate the documentation
dbt docs generate
```

```bash
# Serve the documentation in a web GUI
dbt docs serve

# If you are having port clashes:
dbt docs serve --port 8088
```

The previous command should open a page in your web browser, have a look around! When you're ready, check the "Database" section to find the structure of your BigQuery project, the datasets in the project, the tables and views for each dataset, as well as the documentation and tests of each field.

## 4.2. Source Tests

DBT comes with some built in tests that we can add to our configuration `.yml` files to test for things like uniqueness of values in a column and making sure that there are no null values in a column.

Add tests to your `sources.yml` to ensure the uniqueness and existence of the primary keys of the **product** and **ship** staging models. Some things to think about could be:
- What is the primary key for the model?
- What is the associated test?

💡 Remember that we are testing our **raw source data**, NOT our staging models. This can be useful for identifying if there are issues INSIDE our pipeline or if there are upstream data quality issues.

<details>
<summary markdown="span">💡 Hint</summary>

```yml
# models/source/sources.yml
sources:
  - name: raw
    identifier: raw_gz_data
    tables:
      - name: sales
        identifier: raw_gz_sales
        ...snip...
      - name: product
        identifier: raw_gz_product
        description: products of Greenweez
        columns:
          - name: <your_column_name>
            description: your column description
            data_tests: # apply tests to the column defined in the name key
              - <your_first_test_here>
              - <your_second_test_here>
```

💡 The `tests` key is being depreciated in favour of the more explicit `data_tests` key

</details>

Don't forget to test your models with:

```bash
# Test all models
dbt test
# OR, test a single model
dbt test -m <model_name>
```

<br>

# 🏁 Finishing up

Congratulations! You've now configured your sources and created staging models for those sources!

A recap of what you've done in this challenge.

- DBT is now able to identify the source data, your `gz_raw_data` dataset, and pull data from it.
- You've created 3 models with cleaned and casted values:
    - `stg_raw_sales`
    - `stg_raw_product`
    - `stg_raw_ship`
- You've added documentation and tests to your **sources**.

🧪 To test your code, run:

```bash
# Be sure to run make commands from the challenge root folder!
make test
```

And don't forget to git add, commit, and push your code to GitHub so you can track your progress on Kitt!

<br>
