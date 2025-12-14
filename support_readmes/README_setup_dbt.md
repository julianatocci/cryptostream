# 🎯 Goals

DBT (Data Build Tool) is crucial in data engineering because it enables data teams to transform raw data into clean, structured datasets using plain SQL. It simplifies the process of building and maintaining data transformation pipelines by allowing engineers to write modular, reusable SQL code. DBT also facilitates version control, testing, and documentation, making data workflows more reliable and scalable. By integrating well with modern data warehouses, DBT streamlines data modelling and ensures data quality, helping teams deliver consistent and accurate insights.

The challenges in this unit form a case study that we'll go over in the next challenge. Before we get to that, let's setup DBT, go through some basics, and setup your raw data sources in BigQuery.

In this challenge we'll get DBT setup and start with the build in example before moving onto the next, more substantial challenge.

There are 5 steps in this setup:
1. Setup Big Query and Raw Data
1. Install DBT
1. Build a DBT project (folder structure + config file)
1. Setup the connection configuration with BigQuery (profile file)
1. Run your first DBT models, which will generate *tables* and *views* in BigQuery

<br>

# 1️⃣ Setup Big Query and Raw Data

## 1.1. Download the Raw Data

The data that we'll be using is from Greenweez. There will be more context on the case study in the next challenge. We'll start with three tables to start with. Manually downloading them to your VM then uploading to BigQuery.
- **raw_gz_sales** - Timestamped sales with order_id, product_id, revenue, quantity
- **raw_gz_product** - product_id, purchSE_PRICE
- **raw_gz_ship** - Logistics data with fees and costs

To download the data and save to the `data/` folder run the following commands:

```bash
# Create the folder
mkdir -p data/

# Download the data
curl -o ./data/raw_gz_sales.parquet https://wagon-public-datasets.s3.amazonaws.com/data-engineering/gz_raw_data-raw_gz_sales.parquet
curl -o ./data/raw_gz_product.parquet https://wagon-public-datasets.s3.amazonaws.com/data-engineering/gz_raw_data-raw_gz_product.parquet
curl -o ./data/raw_gz_ship.parquet https://wagon-public-datasets.s3.amazonaws.com/data-engineering/gz_raw_data-raw_gz_ship.parquet
```

<details>
<summary markdown='span'>💡 Peak at the data</summary>
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


## 1.2. Create BigQuery Dataset

We will need to create a dataset in BigQuery to hold our raw data for DBT to source from.

Either:
- Open the [BigQuery console](https://console.cloud.google.com/bigquery) and create a dataset called `raw_gz_data` located in the `EU (multiple regions in the European Union)`
- Use the `bq` command line utility to create a new dataset

<details>
<summary markdown='span'>🎁 bq create dataset</summary>

```bash
# bq mk --location=EU --dataset <project_id>:<dataset_id>
bq mk --location=EU --dataset <your_gcp_project_id>:raw_gz_data
```
</details>

## 1.3. Upload Data to BigQuery

Once the dataset has been created, upload the data you have saved locally in the `data/` folder.

Either:
- Use the [BigQuery console](https://console.cloud.google.com/bigquery) to create a new table and upload the data from file.
- Use the `bq` command line utility to upload each parquet file.

Make sure that the table names are:
- `raw_gz_sales`
- `raw_gz_product`
- `raw_gz_ship`

<details>
<summary markdown='span'>🎁 bq bulk load</summary>

```bash
bq load --autodetect \
    --source_format=PARQUET \
    --location=EU \
    raw_gz_data.<your_table_name> \
    ./path/to/data.parquet
```
</details>

To confirm that the data has been uploaded, head to the GCP console and have a look at the schema and preview for each table.

<details>
<summary markdown='span'>❓ Why not use Airbyte</summary>

This is something that you could absolutely do with Airbyte. Due to the scope of challenges in this module, we recommend you get more hands on with DBT. We've included an optional challenge to do data ingestion with Airbyte so you can try it yourself after you complete the DBT challenges, or as a future reference.
</details>

<br>

# 2️⃣ Install DBT

There are multiple versions of DBT, which vary depending on the database system you're interacting with. We'll be using DBT with BigQuery, so we've added an additional package, `dbt-bigquery` to your `pyproject.toml`.

Make sure DBT is installed by executing `dbt --version` in your terminal. If that command doesn't work, make sure your `pyproject.toml` is up to date, and run `poetry install` in the `01-Setup-DBT` directory to install.

<details>
  <summary markdown='span'>💡 How to check DBT version with poetry</summary>

```bash
poetry show dbt-core
# OR
poetry show | grep dbt
```

</details>

<br>

# 3️⃣ Initialize the DBT project

## 3.1. Initialize `dbt_lewagon` directory

DBT works by creating a DBT project directory inside your project. This directory is where all our models, SQL files, and (most) configuration files will be. We'll call our DBT project: **dbt_lewagon**

Do not worry if you make an error during this process, you can modify any field afterwards.

- In your terminal, go to the **01-Setup-DBT** challenge root. This is where we'll create the DBT project.
- In your terminal, run `dbt init`.
- When prompted:
  - **Enter a name for your project (letters, digits, underscore):** Enter `dbt_lewagon`. If prompted: _The profile dbt_lewagon already exists in ~/.dbt/profiles.yml. Continue and overwrite it?_ Hit `N`.
  - **Which database would you like to use? Enter a number:** Enter `1` (for `bigquery`)
  - **Desired authentication method option (enter a number):** Enter `2` (for `service_account`)
  - **keyfile (/path/to/bigquery/keyfile.json):** Enter the **absolute** path of where you stored your BigQuery service account key that you created during the [Data Engineering setup](https://github.com/lewagon/data-engineering-setup/blob/main/macOS.md), including the file name and extension. It should look similar to: `/home/username/.gcp_keys/le-wagon-de-bootcamp.json`.
  - **project (GCP project id):** Enter your GCP Project ID that you created during the [Data Engineering setup](https://github.com/lewagon/data-engineering-setup/blob/main/macOS.md)
  - **dataset (the name of your dbt dataset):** Call it `dbt_{firstletteroffirstname}{last_name}_day1`. If your name is Taylor Swift your dataset should be: `dbt_tswift_day1`
  - **threads (1 or more):** Enter `2`
  - **job_execution_timeout_seconds [300]:** Enter `300`
  - **Desired location option (enter a number):** Enter `2` for `EU`. This is important!

This should have done 2 things:
- Generated the tree structure needed for the DBT project in the `dbt_lewagon` folder.
- It should have created a `profiles.yml` file at the following location: `~/.dbt/profiles.yml`. If you made an error, open up the `profiles.yml` and edit the appropriate field.

## 3.2. Verify the setup of `profiles.yml`

DBT Core works on the concepts of **profiles**. Typically for each DBT project you would have one **profile**, and generally one **profile** for each business unit or team that you work with.

Each **profile** can contain one or more *targets*. Each *target* defines what data warehouse you are using, authentication credentials, output dataset name, and data location. With different *targets* we can apply the same pipeline to read from different source datasets and write to different output datasets. It's how we differentiate between development and production.

If that's a bit much at the moment, don't worry too much about it, we'll go deeper into **profiles** and *targets* during the recap.

For more information about the DBT profile, have a look at the documentation:
- `profiles.yml` documentation [at this link](https://docs.getdbt.com/reference/profiles.yml)
- BigQuery dbt profile documentation [at this link](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile)

Let's check that the `profiles.yml` file is configured correctly:
- By default, your DBT profile will be stored in your VM's **home directory** at `~/.dbt/profiles.yml`. Open it by running: `code ~/.dbt/profiles.yml`. You should be able to see all the configuration you've set when creating the DBT project.
- Run `make test` to make sure the setup of your profile is correct. (The `test_dbt_profile` tests should all be green). Watch out with the levels of indentation in your `profiles.yml` file
- Push to git.

## 3.3. Verify `dbt_project.yml` file

_No action item in this section - we're just providing context._

Open the `dbt_lewagon` folder, and open the `dbt_project.yml` file in this folder:
- Verify that the project name (~line 5) is correct: `name: 'dbt_lewagon'`
- Verify that the profile name (~line 10) is correct: `profile: 'dbt_lewagon'`
- At the bottom of the file, make sure the `models` parameter refers to your project name:

```yml
models:
  dbt_lewagon: # <-- should match the `name` tag
  # Config indicated by + and applies to all files under models/example/
  example:
    +materialized: view
```

We're defining the default behaviour of our models. Models defined in `models/example/`, and any subfolder created in `example/`, will be materialized (created) as a `view` in our data warehouse unless specified otherwise.

<details>
<summary markdown='span'>💡 What does the `+` symbol mean again?</summary>

In the `dbt_project.yml` file, the `+` symbol, like in `+materialized` or `+schema`, is to avoid collision between resource paths (e.g. a folder in `models`) and a config name. Imagine you had a model folder named `tags`: `tags` is also a config you could use in `dbt_project.yml`. By writing `+tags` DBT would know you're referring to the config, not to the model folder. DBT recommends using the `+` prefix for model configurations in your `dbt_project.yml`. It helps clearly indicate that it's a config and not a resource path.

The `+` prefix means something different in the context of terminal commands. In the following example:

```bash
dbt run --select my_first_dbt_model+
```

DBT will run the `my_first_dbt_model` model and all of its downstream models.

</details>

## 3.4. Verify initial tree structure of the DBT project

💡 No action item in this section - we're just providing context.

In the `models` folder there should be two files:
- `/example/my_first_dbt_model.sql`
- `/example/my_second_dbt_model.sql`
- Having a look at the two example models:
    - `my_first_dbt_model.sql` is generating some dummy `source` data, through a CTE, and is then storing this result in a `table` called `my_first_dbt_model`.
        - The block at the top of the model defines the output of the model should be a table, over-riding the default materialization.
        - The table created in BigQuery will be named `my_first_dbt_model` - DBT uses the name of the file without the `.sql` on the end.
    - `my_second_dbt_model.sql` references to `my_first_dbt_model` model.
        - The materialization is not specified so it will be stored as the default materialization defined in the `dbt_project.yml` - a view

Let's explore the `/models/example/schema.yml` file. In here you can find:
- The list of models in the folder
- A high level description of each model
- For each column of each model, the ability to document:
    - A description of the column
    - Tests, or constraints - like unicity, the field not being null, etc.

<br>

# 4️⃣ Enhance DBT project tree

❗ **From now on, all command lines assume you're executing them from your `dbt_lewagon` directory** ❗

### 4.1. Organising DBT Models

Let's organize our project a bit. We'll be working with four layers, similar to a bronze, silver, gold architecture with an extra layer for sourcing:
- `source` data - where we'll source our data from
- `staging` data - a cleaner version of the `source` data, with some casting and column renaming
- `intermediate` data - where we'll perform aggregations and transformations
- `mart` data - which contains clean metrics that business stakeholders can report on.

Let's make sure the `.sql` and `.yaml` files corresponding to each layer are in a corresponding folder. `cd` to the `dbt_lewagon` directory. From there, execute the following command:

```bash
mkdir models/source;
mkdir models/staging;
mkdir models/intermediate;
mkdir -p models/mart/finance;
```

We built the above structure (`source`, `staging`, `intermediate`, and `mart` folders) for a specific reason. When we organise our models in directories, our tables/views in BigQuery will be prefixed to make them more identifiable. We want `.sql` models:
- In our `staging` directory to create schemas in BigQuery that are prefixed with `stg_`
- In our `intermediate` directory to create schemas prefixed with `int_`
- In our `mart/finance` directory to create schemas to serve the finance team

In our `source` folder, we will only have a configuration file: `sources.yaml`. We don't need SQL models for this because we'll read directly from tables in our raw dataset, `raw_gz_data`, on BigQuery.

### 4.2. Confirm DBT project tree

Your DBT project structure should now look like this. The `target` folder may or may not exist or have sub directories, depending on if you've already run some models - don't worry too much about it for now.

```bash
.
├── analyses
├── macros
├── models
│   ├── example
│   │   ├── my_first_dbt_model.sql
│   │   ├── my_second_dbt_model.sql
│   │   └── schema.yml
│   ├── intermediate
│   ├── mart
│   │   └── finance
│   ├── source
│   └── staging
├── seeds
├── snapshots
├── target
├── tests
├── dbt_project.yml
└── README.md
```

<br>

# 5️⃣ Run your first models and data tests

Your setup is now ready to run your first models!

Execute the following command to generate your models:
```bash
dbt run -m my_first_dbt_model
```

Where do you think this model will be created in BigQuery?

When you're ready, run the second model:

```bash
dbt run -m my_second_dbt_model
```
You might have an error 🤔 Try and resolve it in the model and rerun.

As seen in `dbt_lewagon/models/example/schema.yml`:
  - `id` in `my_first_dbt_model` should be `unique` and `not_null`
  - `id` in `my_second_dbt_model` should be `unique` and `not_null`

Let's verify that by running those tests. As separate lines in your terminal run:

```bash
dbt test -m my_first_dbt_model
dbt test -m my_second_dbt_model
```

`my_second_dbt_model` is dependent on `my_first_dbt_model`. If you want to refresh `my_second_dbt_model` and all of it's upstream dependencies, like `my_first_dbt_model`, you can run:

```bash
dbt run -m +my_second_dbt_model
```

In the BigQuery interface, in your personal dataset, check that:
  - `my_first_dbt_model` has materialized as a table
  - `my_second_dbt_model` has materialized as a view - this matches with the config in each of the models

<br>

# 🏁 Finishing up

Congratulations on setting up your first DBT project and running some models! The setup was lengthy, but this is a great foundation for any future DBT project you may have!

🧪 Test your setup with:

```bash
# Be sure to run make commands from the challenge root folder!
make test
```

And make sure to add, commit, and push your code up to github so your progress is tracked on Kitt!

<br>

## Some DBT Tips 💡

### Running Models and Tests

If you want to run all your models, or run all your tests, there is no need to specify the model with the `-m xxxx` or `--select xxxx` parameter:
- `dbt run` runs all your DBT models
- `dbt test` runs all tests defined for the project
- `dbt build` runs `dbt run` for every model, followed by `dbt test` for that model (if a test fails, DBT stops downstream execution of dependent models)

### Debugging with Compiled SQL in the BigQuery SQL Editor

If you want to debug a model in BigQuery, have a look at `target/compiled/dbt_lewagon/models/` and click on a folder and have a look at some of the SQL models. This is the **compiled SQL** that DBT generates from your models and configuration files. You can copy and paste these queries directly into the BigQuery SQL editor and diagnose what is happening.

To compile your models **WITHOUT** running them, use: `dbt compile`

### Understanding DBT Tests

Remember the tests you implemented in SQL in the **SQL-Advanced** module? The concept of a SQL test was: *it fails if the SQL query returns at least 1 record.*

Go and check the actual "compiled" tests created by DBT by going to `target/run/dbt_lewagon/models/example/schema.yml/`. This is where you'll find the tests defined directly in the `schema.yml` file. If you open one of the unicity tests, like `unique_my_first_dbt_model_id.sql`, you'll see it has the classic structure:

```sql
select
    unique_field,
    count(*) as n_records
from dbt_test__target
group by unique_field
having count(*) > 1
```

<br>
