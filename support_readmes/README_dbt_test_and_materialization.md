# 🎯 Goal

You've created your pipeline with a staging, intermediate, and mart layers. But we can add some more robustness to our pipeline with:
- Tests and documentation.
- Add default materialization for different layers of our pipeline.
- Apply the [Principle of Least Privilege](https://en.wikipedia.org/wiki/Principle_of_least_privilege) so that our mart users only have access to the mart data they need and not the staging and intermediate data.

This section has three main goals:
- Create configuration files for the different pipeline layers and add tests and documentation to our pipeline.
- Add default materialization to the different layers of our pipeline.
- Configure our pipeline to materialize mart models into a different BigQuery dataset

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

# 1️⃣ Testing and Documentation

We added documentation and tests to our **sources**, but don't have any for our **staging**, **intermediate**, and **mart** models.

Tests are essential to our pipeline to ensure the quality of the data coming through and to make sure things are working as intended.

Documentation of any data pipeline is essential for:

- Understanding models created by other people
- Comprehending complex transformations and lineage
- Maintaining a growing number of automated pipelines
- Quickly debugging errors in the data pipeline

By adding documentation as code, we get a few instant benefits:

- Documentation is now standardized across models and pipelines
- Our documentation is now in version control
- DBT can serve nicely formatted documentation to be viewed in a webpage, accessible by everyone in the company.

## 1.1. Document Staging Models

We have a configuration file for our **sources**: `sources.yml`. It wouldn't make sense to extend this configuration file with information about our **staging** models. Let's create a new configuration file specific to **staging**.

- Under the `models/staging/` directory, create a file called: `staging_models.yml`
- Using [DBT's documentation at this link](https://docs.getdbt.com/docs/build/documentation) on how to properly document a model, document your three staging models:
    - Provide a high level description of the model itself
    - Provide a high level description of all the columns
- Do not worry about tests for your staging models in this challenge, we've tested our sources.

<details>
<summary markdown='span'>💡 Hint</summary>

Check previous challenges to see if you can find column descriptions. The descriptions for your staging models will be very similar to the descriptions of your **sources**.
</details>

🧪 Test your `staging_models.yml` config with:

```bash
make test_staging_config
```

## 1.2. Document and Test Intermediate Models

Similarly for your **intermediate** models, we'll create a configuration file just for this layer.

- Under the `models/intermediate/` directory, create a file called: `intermediate_models.yml`
- Using [DBT's documentation](https://docs.getdbt.com/docs/build/documentation) on how to properly document a model, document your three intermediate models:
    - Provide a high level description of the model itself
    - Provide a high level description of all the columns
- Apply the following tests for each model. You should not write custom tests, meaning there should not be any piece of SQL code written for those tests, they should be implemented in the `intermediate_models.yml` file directly.
    - `int_orders_margin` - Test the primary key is unique and always populated
    - `int_orders_operational`- Test the primary key is unique and always populated
    - `int_sales_margin`:
        - Test that the `products_id` and `orders_id` are always populated
        - Test that the combination of the `products_id` and `orders_id` column is unique

<details>
<summary markdown='span'>💡 Hints</summary>

Check previous challenges to see if you can find column descriptions.

For the uniqueness of the concatenation of two columns, have a look around for a pre-build test. You will need to install a DBT package that enables you to configure this type of test: [dbt_utils](https://hub.getdbt.com/dbt-labs/dbt_utils/0.8.6/). You'll need to create a `packages.yml` file at the same level as your `dbt_project.yml` file, and run `dbt deps` to install the external package.
</details>

🧪 Test your `intermediate_models.yml` config with:

```bash
make test_intermediate_config
```

## 1.3. Document and Test Mart Model

Finally, add a configuration file for your **mart** model.

- Under the `models/mart/` directory, create a file called: `mart_models.yml`.
- Using [DBT's documentation](https://docs.getdbt.com/docs/build/documentation) on how to properly document a model, document your mart model:
    - Provide a high level description of the model itself
    - Provide a high level description of all the columns
- Apply the following tests for the mart model. You should not write custom tests, meaning there should not be any piece of SQL code written for those tests, they should be implemented in the `mart_models.yml` file directly.
    - Test the date field is unique and always populated

❗ You could create a `.yml` for the finance sub folder, or for any number of sub folders in a model layer. Assume in this case study that a single configuration for the mart layer is adequate.

<details>
<summary markdown='span'>💡 Hint</summary>
Check previous challenges to see if you can find column descriptions.
</details>

<br>

🧪 To test to see if you have the correct tests and fully documented your models, run:

```bash
make test_mart_config
```

Even if we weren't going to add documentation (you definitely should!) and tests, it is still best practice to add a `.yml` in each subdirectory of `models/`. In the wild, these files may be named just `models.yml` or `schema.yml` for each subdirectory in `models/`, but we want to be explicit. Under the hood DBT doesn't care what you name the files.

<br>

# 2️⃣ Model Materialization

We only have one **mart** model at the moment, **finance_days**. What if we wanted this mart model and any others we create to materialize as a table in BigQuery? Defining materialization in each model is repetitive and prone to human error. Let’s set the default materialization for our mart layer to be a table.

💡 Why would we want to materialize a mart model as a table instead of a view?

Views are stored as an underlying query, not as data like in a table. Therefore, to retrieve data from a view, the underlying query needs to be run. If this model is serving BI tools or other software, it may be accessed multiple times a day, and the cost of computing the underlying query each time the data is accessed adds up. In this scenario, it would be worth storing the model as a table, paying for the cost of storing the data.

## 2.1. Changing Default Materialization

We can change the default materialization of our DBT models by changing the configuration in the `dbt_project.yml` file in your `dbt_lewagon/` directory.

Edit the section at the bottom of the `dbt_project.yml` to set the default materialization of your **mart layer** to be a **table**.

<details>
<summary markdown='span'>💡 Hint</summary>

```yaml
# dbt_project.yml

models:
  dbt_lewagon: # <-- make sure this value matches the value of `name` on line 5
    mart:
      +materialized: ...
```

💡 What does the `+` symbol in `dbt_project.yml` mean again?

In DBT, the `+` symbol, like in `+materialized` or `+schema`, applies the configuration to that layer of models and **inherits down** to all nested sub-directories. [DBT recommends](https://docs.getdbt.com/reference/resource-configs/plus-prefix) using the `+` prefix for model configurations in your `dbt_project.yml`. It helps clearly indicate the intention for models and their materialization.

</details>

When you think you have the correct configuration in `dbt_project.yml`, remove the `{{ config(materialized="view") }}` block from your `finance_days.sql` model. **Run** or **build** your pipeline and check BigQuery, **finance_days** should have materialized as a table and look similar to the below:

❗ Note: Because of how Jinja templating works, the configuration block at the top of any individual model will still be executed even if you comment it out. You must delete it!

<img src="https://wagon-public-datasets.s3.amazonaws.com/data-engineering/030204-dbt-bigquery-finance-materialization.png">

The materialization wil apply to all models inside the mart directory and all of its sub-directories. You can override this materialization in a single model with: `{{ config(materialized="view") }}` block at the top of a SQL file.

## 2.2. Separate Mart Dataset

At the moment our **mart** model is being created in the same BigQuery dataset as the **staging** and **intermediate** models. Knowing that it's relatively easy to control IAM access at the dataset level in BigQuery, the finance team does not need access to the staging and intermediate models, so let's apply the principle of least privilege and expose the **finance_days** model in a separate dataset.

The way to achieve this is by editing your `dbt_project.yml`. Remember that in DBT, a schema is a BigQuery dataset. Create a configuration so that all the models in `models/mart/finance/` are created in a new BigQuery dataset with the name: `dbt_<first_letter_and_last_name>_day1_finance`. So if your name was Taylor Swift, the dataset would be named: `dbt_tswift_day1_finance`.

<details>
<summary markdown='span'>💡 Hint</summary>

```yaml
# dbt_project.yml

models:
  dbt_lewagon: # <-- make sure this value matches the value of `name` on line 5
    mart:
      +materialized: ...
      finance:
        +schema: ...
```

</details>

When you think you have the correct configuration, **run** or **build** your pipeline and check BigQuery. Your `finance_days` model should have materialized in a new dataset, similar to the below:

<img src="https://wagon-public-datasets.s3.amazonaws.com/data-engineering/0302-dbt-bigquery-mart-dataset.png">

<br>

# 🏁 Finishing up

Congratulations! We've accomplished a lot in this challenge to make your pipeline more robust and how to expose only the **mart models** to our finance team. We have:
- More thoroughly documented and tested our pipeline.
- Set default model materialization for different layers of our pipeline.
- Materialized different layers of or pipeline into different warehouse datasets.

🧪 To test all of your code, run:

```bash
# Be sure to run make commands from the challenge root folder!
make test
```

Don't forget to git add, commit, and push your code to github so you can track your progress on Kitt!

<br>
