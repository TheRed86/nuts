##################################
How to set up the NUTS environment
##################################

The Alia-Space NUTS project provides an online tool to allow users obtaining merged and cropped data by selecting
a data product, a Region of Interest (ROI), and a time period.

Set up the CONDA environment
----------------------------


To work with NUTS, start by creating a new conda environment. Let's call it `nuts_env`, but
any valid name would do (change the following instructions accordingly):

    conda create -n nuts_env python=3.8 geopandas owslib

Activate the environment:

    conda activate nuts_env

Once the environment is activated, install the required libraries to work with Prefect and the Postgres DB:

    conda install -y -c conda-forge prefect psycopg2

You are almost ready to execute your flow!

In the following paragraphs, we show you how to start a Prefect flow and an example of a request.

Prefect set up
--------------
Open a terminal and navigate to the repository folder. You should see the `docker-compose.yaml`, the `create_db.sh`,
and the `SENTINEL_workflow.py`/`DEM_workflow.py` files in the current folder. If so, you are in the right place.

Once there, you have to execute the following commands:

    docker-compose up

Open a new terminal (the docker output occupies the previous one) and go to the same folder.
The first time you start up the system, execute the following commands:

    ./create_db.sh

    prefect server create-tenant --name NUTS_tenant

Finally, go to the page `localhost:8080` in a local browser and create a new project in the Prefect UI.
Call the `new project` `NUTS`.

Now, you are ready to execute the last two commands in a terminal pointing to the repository path:

    python SENTINEL_workflow.py

    prefect agent local start

All you have to do now is connect to the DB to fill your request!

**NOTE:** if you want to run the DEM flow, use the following command instead of `python NUTS_workflow.py`:

    python DEM_workflow.py

Connect to the DB
-----------------

Open the Postgres IDE and connect to the NUTS DB.

    name: nuts_db
    host: localhost
    port: 5432
    username: nuts_user
    password: nuts_password

Once connected, you should see the following tables:
    * user
    * order_table
    * job
    * source_product



Case study
----------


To enable a request in the Prefect flow, you must fill the `user` table and the `order_table` one in the DB.
Specifically:

    *user*:
        user_id (a unique number is automatically assigned for each DB row inserting the name)

        name

        email address


    *order_table*:
        user_id (it should correspond to the unique user identifier)

        shape_layer (i.e., NUTS_2)

        shape_roi (i.e., Lazio)

        source_mission (i.e., Sentinel-2)

        source_data_type (i.e., S2MSI1C)

        start_date (i.e., 2021-09-01)

        stop_date (i.e., 2021-09-05)

This request will make you download 28 products of Sentinel-2 data.

To execute a run in which DEM data is requested, follow the example below:

    *user*:
        user_id (a unique number is automatically assigned for each DB row inserting the name)

        name

        email address


    *order_table*:
        user_id (it should correspond to the unique user identifier)

        shape_layer (i.e., GADM_1)

        shape_roi (i.e., Qina)

        source_mission (i.e., DEM)

        source_data_type (i.e., DS)

        start_date (i.e., 2020-01-01)

        stop_date (i.e., 2020-12-31)


Prefect is ready to execute your order: in the Prefect web interface, go to `flow` and select `NUTS_workflow`.
Push the `RUN` button and insert the `order_id` associated to the DB request you want to perform.

Have fun!!




