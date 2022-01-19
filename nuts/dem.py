import re
import os
from osgeo import gdal
from owslib.wms import WebMapService
import prefect
from prefect import task
import psycopg2
import numpy as np
import datetime


@task
def downloadDEMproduct(orderid, shapeBoundaries):
    # logging connection
    logger = prefect.context.get("logger")

    # establish connection with db
    conn = psycopg2.connect(
        host='localhost',
        database='nuts_db',
        user='nuts_user',
        password='nuts_password',
        port=5432
    )
    cursor = conn.cursor()

    # read inputs from order_table
    order_request = "SELECT * FROM order_table WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(order_request)
    order = cursor.fetchone()
    shape_roi = order[3]
    shpid = re.sub(r'[^\w]', '', shape_roi)
    data_type = order[5]

    sensing_start = order[6]
    sensing_stop = order[7]

    # create list of years
    year_range = [year for year in range(sensing_start.year, sensing_stop.year + 1)]
    product_list = []

    # iter over years
    for year in year_range:
        # select product based on data type
        if data_type == 'DS':
            layer_name = f'gds:Africa_DEM_{year}_DS'
            output_name = f'Africa_DEM_{year}_DS_{shpid}.tif'
        elif data_type == 'gray':
            layer_name = f'gds:Africa_DEM_{year}'
            output_name = f'Africa_DEM_{year}_{shpid}.tif'

        logger.info('Retrieving ' + str(layer_name))

        # update db with new order and retrieve job id
        start_date = datetime.date(year, 1, 1)
        stop_date = datetime.date(year, 12, 31)
        insert_jobs = "INSERT INTO job (order_id, start_date, end_date) VALUES (%s, %s, %s) RETURNING job_id"  # 888
        records_to_insert = (orderid, start_date, stop_date)
        cursor.execute(insert_jobs, records_to_insert)
        conn.commit()
        jobid = cursor.fetchone()[0]

        # download product if not already available in cache_folder
        if os.path.isfile(output_name):
            logger.info("Product already downloaded!")
        else:
            wms = WebMapService('https://geoserver.alia-space.com/geoserver/gds/wms', version='1.3.0')
            img = wms.getmap(layers=[layer_name],
                             srs='EPSG:4326',
                             bbox=(shapeBoundaries[0], shapeBoundaries[1], shapeBoundaries[2], shapeBoundaries[3]),
                             size=(704, 768),
                             format='image/geotiff'
                             )
            with open(os.path.join('cache_folder', output_name), 'wb') as out:
                out.write(img.read())

            logger.info('Output available as ' + str(output_name))

        if os.path.exists(os.path.join('cache_folder', output_name)):
            download_status = 'downloaded'
        else:
            download_status = 'failed'

        # update db with products and retrieve product id
        insert_product = "INSERT INTO source_product (job_id, uri, product_name, status) VALUES (%s, %s, %s, %s) RETURNING source_product_id"
        product_to_insert = (jobid, layer_name, output_name, download_status)
        cursor.execute(insert_product, product_to_insert)
        conn.commit()
        prod_index = cursor.fetchone()[0]
        product_list.append(prod_index)

        return product_list


@task
def cropDEMproducts(jobid):
    # logging connection
    logger = prefect.context.get("logger")

    # establish connection with db
    conn = psycopg2.connect(
        host='localhost',
        database='nuts_db',
        user='nuts_user',
        password='nuts_password',
        port=5432
    )
    cursor = conn.cursor()

    # retrieve the orderid and year from db
    orderid_request = "SELECT * FROM job WHERE job_id = '" + str(jobid) + "'"
    cursor.execute(orderid_request)
    order = cursor.fetchone()
    orderid = order[1]
    year = order[2].year

    # read inputs from order_table
    order_request = "SELECT * FROM order_table WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(order_request)
    order = cursor.fetchone()
    shape_roi = order[3]
    data_type = order[5]

    # point the shapefile
    shpid = re.sub(r'[^\w]', '', shape_roi)
    shp_file = f'{os.getcwd()}/shapefiles/shape_{shpid}.shp'
    logger.info("The chosen ROI is " + str(shp_file))

    # create the job dir
    job_folder = f'{os.getcwd()}/working_dir/job_{jobid}'
    if not os.path.exists(job_folder):
        os.makedirs(job_folder)

    # select input and output file based on product type
    if data_type == 'DS':
        output_file = f'{job_folder}/Africa_DEM_{year}_DS_{shpid}.tif'
        original_dem = f'cache_folder/Africa_DEM_{year}_DS_{shpid}.tif'
    elif data_type == 'gray':
        output_file = f'{job_folder}/Africa_DEM_{year}_{shpid}.tif'
        original_dem = f'cache_folder/Africa_DEM_{year}_{shpid}.tif'

    # crop input data as for shapefile
    ds = gdal.Open(original_dem, 1)
    gdal.Warp(output_file, ds, format='GTiff', cutlineDSName=shp_file, cutlineLayer=f'shape_{shpid}', cropToCutline=True, dstNodata=np.nan)

    # update db with job status
    if not os.path.exists(output_file):
        status = 'failed'
    else:
        status = 'completed'

    insert_job_status = "UPDATE job SET status = %s WHERE job_id = %s"
    cursor.execute(insert_job_status, (status, jobid))
    conn.commit()
    logger.info("Database updated with job status.")

    return jobid

