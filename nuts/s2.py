from prefect import task
import os
import zipfile
from osgeo import gdal
import prefect
import psycopg2
import glob
from pathlib import Path
import numpy as np
import re
from datetime import datetime


@task
def mergeProducts(jobid):

    # logging connection
    logger = prefect.context.get("logger")
    logger.info(" Currently on job: " + str(jobid))

    # establish connection with db
    conn = psycopg2.connect(
        host='localhost',
        database='nuts_db',
        user='nuts_user',
        password='nuts_password',
        port=5432
    )
    cursor = conn.cursor()

    # get the products name from jobid
    order_request = "SELECT product_name FROM source_product WHERE job_id = '" + str(jobid) + "'"
    cursor.execute(order_request)
    product_list = cursor.fetchall()
    product_name_list = []
    for i in product_list:
        product_name_list.append(i[0])

    # retrieve the orderid from db
    orderid_request = "SELECT order_id FROM job WHERE job_id = '" + str(jobid) + "'"
    cursor.execute(orderid_request)
    orderid = cursor.fetchone()[0]

    # retrieve the shapefile path and the mission from db
    request = "SELECT * FROM order_table WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    shp = req[3]
    datatype = req[5]

    # redefine the shapefile path for the gdal command
    shpid = re.sub(r'[^\w]', '', shp)
    shp_file = f'{os.getcwd()}/shapefiles/shape_{shpid}.shp'
    logger.info("The chosen ROI is " + str(shp_file))

    logger.info("Merging the following products" + str(product_name_list))

    products_folder = os.path.join(os.getcwd(), 'cache_folder/')

    # create job folder and output folder
    job_folder = f'{os.getcwd()}/working_dir/job_{jobid}'
    if not os.path.exists(job_folder):
        os.makedirs(job_folder)

    # unzip downloaded products belonging to the current job
    extension = ".zip"
    for item in product_name_list:  # loop through items in dir
        logger.info("Unzipping " + str(item))
        file_name = item+extension  # add zip extension
        file_name = os.path.join(products_folder, file_name)
        zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
        zip_ref.extractall(job_folder)  # extract file to dir
        zip_ref.close()  # close file

    # define the list of bands to work on Sentinel-2 data
    if 'S2MSI1C' in datatype:
        bands_list = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12', 'TCI']
    elif 'S2MSI2A' in datatype:
        bands_list = ['AOT_10m', 'B02_10m', 'B03_10m', 'B04_10m', 'B08_10m', 'TCI_10m', 'WVP_10m', 'AOT_20m', 'B02_20m',
                      'B03_20m', 'B04_20m', 'B05_20m', 'B06_20m', 'B07_20m', 'B11_20m', 'B12_20m', 'B8A_20m', 'SCL_20m', 'TCI_20m', 'WVP_20m',
                      'AOT_60m', 'B01_60m', 'B02_60m', 'B03_20m', 'B04_20m', 'B05_20m', 'B06_20m', 'B07_20m', 'B09_60m', 'B11_60m', 'B12_60m', 'B8A_60m', 'SCL_60m', 'TCI_60m', 'WVP_60m'
                      ]
    # create a vrt file for each band of each product to reproject products at different UTM zone
    for band in bands_list:
        logger.info('Processing band ' + str(band))

        if 'S2MSI1C' in datatype:
            list_images = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*{band}.jp2")
        elif 'S2MSI2A' in datatype:
            list_images = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*/*{band}.jp2")
        matchname = re.search(r'\d{4}\d{2}\d{2}', list_images[0])
        logger.info("date " + str(matchname))
        cur_date = datetime.strptime(matchname.group(), '%Y%m%d').date()
        cur_date = str(cur_date).split("-")
        output_vrt = f'{job_folder}/S2B_{datatype}_{cur_date[0]}{cur_date[1]}{cur_date[2]}_{band}_{shpid}.vrt'

        for i in list_images:
            tmp_out = i.split('.jp2')
            tmp_out = tmp_out[0] + '.vrt'
            ds = gdal.Open(i)
            gdal.Warp(tmp_out, ds, dstSRS='epsg:4326', dstNodata=np.nan)

        if 'S2MSI1C' in datatype:
            list_images_vrt = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*{band}.vrt")
        elif 'S2MSI2A' in datatype:
            list_images_vrt = glob.glob(f"{job_folder}/*.SAFE/GRANULE/*/IMG_DATA/*/*{band}.vrt")

        # create a unique VRT file with all tiles for each band
        vrt = gdal.BuildVRT(output_vrt, list_images_vrt)
        vrt = None
    # crop merged products based on selected ROI
    for pp in Path(job_folder).rglob('S2B_*.vrt'):
        logger.info('Cropping band ' + str(band))
        output_file = str(pp).split('.vrt')
        output_file = output_file[0]+'.tif'
        ds = gdal.Open(str(pp))
        gdal.Warp(output_file, ds,  format='GTiff', cutlineDSName=shp_file, cutlineLayer=f'shape_{shpid}', cropToCutline=True, dstNodata=np.nan)

    insert_job_status = "UPDATE job SET status = %s WHERE job_id = %s"
    cursor.execute(insert_job_status, ("completed", jobid))
    conn.commit()
    logger.info("Database updated with job status.")

    return jobid
