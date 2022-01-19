import prefect
from prefect import task
import psycopg2
import os
import re
import glob
import requests
import geopandas as gp
from fiona import BytesCollection
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


@task
def retrieveShape(orderid):
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
    shape_layer = order[2]
    shape_roi = order[3]

    if 'NUTS' in shape_layer:
        level = shape_layer.split('_')[1]
        layer = f'nuts:NUTS_RG_01M_2021_3857_LEVL_{level}'
        feature = shape_roi
        cql = 'NAME_LATN'
        crs = 'epsg:3857'
    elif 'GADM' in shape_layer:
        level = shape_layer.split('_')[1]
        layer = f'gadm:gadm36_{level}'
        feature = shape_roi
        cql = f'NAME_{level}'
        crs = 'epsg:3857'

    logger.info("Retrieving shapefile for " + str(shape_roi))

    def wfs2gp_df(url, layer_name, feature_id, cql_name, wfs_version="2.0.0"):
        with BytesCollection(requests.get(
                f"{url}?service=wfs&version={wfs_version}&request=GetFeature&typeNames={layer_name}&CQL_FILTER={cql_name}='{feature_id}'"
        ).content) as f:
            df = gp.GeoDataFrame.from_features(f, crs=crs)
        return df

    geo_data = wfs2gp_df('https://geoserver.alia-space.com/geoserver/ows', layer, feature, cql_name=cql)
    geo_data.crs = crs

    logger.info("Shapefile obtained!")

    # save shapefile to disk (in shapefiles folder)
    path = os.getcwd()
    shapefile_dir = f'{path}/shapefiles'
    if not os.path.exists(shapefile_dir):
        os.makedirs(shapefile_dir)
    shpid = re.sub(r'[^\w]', '', shape_roi)
    shapefile_path = f'{shapefile_dir}/shape_{shpid}.shp'

    geo_data.to_file(shapefile_path)
    geo_data_4326 = geo_data.to_crs(epsg=4326)
    geo_data_boundary = geo_data_4326.total_bounds
    logger.info("Shapefile obtained!")


    return geo_data_boundary

@task
def organizeJobs(downloaded_products, orderid):

    # establish connection with db
    conn = psycopg2.connect(
        host='localhost',
        database='nuts_db',
        user='nuts_user',
        password='nuts_password',
        port=5432
    )
    cursor = conn.cursor()

    # logging connection
    logger = prefect.context.get("logger")

    logger.info("Working on " + str(downloaded_products))

    # retrieve jobs from db based on order-id
    job_request = "SELECT job_id FROM job WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(job_request)
    jobs = cursor.fetchall()
    logger.info("The products belong to job=" + str(jobs))

    job_list = []
    for job in jobs:
        job_list.append(job[0])

    return job_list


@task
def notifyResult(jobid):
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

    # retrieve the job status and orderid from db to retrieve userid
    request = "SELECT * FROM job WHERE job_id = '" + str(jobid[0]) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    job_status = req[4]
    orderid = req[1]

    # retrieve the userid from db to get email address
    request = "SELECT * FROM order_table WHERE order_id = '" + str(orderid) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    userid = req[1]

    # retrieve the email address from db to notify user
    request = "SELECT * FROM public.user WHERE user_id = '" + str(userid) + "'"
    cursor.execute(request)
    req = cursor.fetchone()
    username = req[1]
    receiver_email = req[2]

    # move the tif files from job_x to order_x folder and remove the former
    output_folder = os.path.join(os.getcwd(), f'products_ready/order_{orderid}/')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for j in jobid:
        job_folder = f'{os.getcwd()}/working_dir/job_{j}'
        list_images_tif = glob.glob(f"{job_folder}/*.tif")
        for tif in list_images_tif:
            cmd = f'mv {tif} {output_folder}/'
            os.system(cmd)
            logger.info("File " + str(tif) + "moved in folder " + str(output_folder))
        cmd = f'rm -r {job_folder}'
        os.system(cmd)

    # email parameters
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = 'testnuts.project@gmail.com'
    password = 'testnutsproject.2021'

    message = MIMEMultipart("alternative")
    message["Subject"] = "multipart test"
    message["From"] = sender_email
    message["To"] = receiver_email

    # Create the plain-text and HTML version of your message
    text = f"""\
    Hi {username},
    All the products you requested are ready for you!

    See you account at dsm2.alia-space.com/8080/map

    Best regards,
    Nuts team"""
    html = f"""\
    <html>
      <body>
        <p>Hi, {username}<br>
           All the products you requested are ready for you!<br>
           See your account at
           <a href="dsm2.alia-space.com/8080/map">DSM2 Alia-Space</a> 
        </p>
      </body>
    </html>
    """

    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    # Create a secure SSL context
    context = ssl.create_default_context()

    if job_status == 'completed':
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())

        insert_order_status = "UPDATE order_table SET status = %s WHERE order_id = %s"
        cursor.execute(insert_order_status, ("user notified", orderid))
        conn.commit()
        logger.info("Database updated with order status.")
    else:
        insert_order_status = "UPDATE order_table SET status = %s WHERE order_id = %s"
        cursor.execute(insert_order_status, ("failed", orderid))
        conn.commit()
        logger.info("Database updated with order status (failed).")
