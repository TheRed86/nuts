import prefect
from prefect import task
import psycopg2
import os
import csv
import datetime
from time import sleep


@task
def queryCatalog(orderid, shapeBoundaries):
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
    mission = order[4]
    data_type = order[5]
    logger.info("Data type:" + str(mission) + " - " + str(data_type))

    # create a list of datetime between sensing_start and sensing_stop user dates
    sensing_start = order[6]
    sensing_stop = order[7]
    delta = sensing_stop - sensing_start  # returns timedelta
    start_date_list = []
    stop_date_list = []
    for i in range(delta.days + 1):
        day = sensing_start + datetime.timedelta(days=i)
        start_date_list.append(datetime.datetime.strftime(day, '%Y-%m-%dT00:00:00.000Z'))
        stop_date_list.append(datetime.datetime.strftime(day, '%Y-%m-%dT23:59:59.999Z'))

    product_list = []

    # Search products on catalogue for each datetime
    for i in range(len(start_date_list)):
        logger.info("Querying for the time range:" + str(start_date_list[i]) + " - " + str(stop_date_list[i]))

        # hardcoded inputs for search on hub
        usr_name = 'gianpieroizzo'
        usr_psw = 'passwordXmonica'
        hub = 'https://scihub.copernicus.eu/dhus'
        # Use dhusget method for the query
        page = 1
        url = f'./dhusget.sh -d {hub} -u {usr_name} -p {usr_psw} -m {mission} -S {start_date_list[i]} -E {stop_date_list[i]} -c {shapeBoundaries[0]},{shapeBoundaries[1]}:{shapeBoundaries[2]},{shapeBoundaries[3]} -T {data_type} -l 100 -P {page}'  # -q {query_result} -C {products_list} '
        os.system(url)
        logger.info('Query done! Writing data on db ... ')
        num_lines = sum(1 for line in open('products-list.csv'))
        logger.info('Found: ' + str(num_lines) + ' on page ' + str(page))
        # update db with entries if products have been found
        if num_lines != 0:
            insert_jobs = "INSERT INTO job (order_id, start_date, end_date) VALUES (%s, %s, %s) RETURNING job_id"  # 888
            records_to_insert = (orderid, start_date_list[i], stop_date_list[i])
            cursor.execute(insert_jobs, records_to_insert)
            conn.commit()
            jobid = cursor.fetchone()[0]
            # read products from query resulting file and put them in db
            with open('products-list.csv', 'r') as prodlist:
                csv_reader = csv.reader(prodlist, delimiter=',')
                for row in csv_reader:
                    insert_product = "INSERT INTO source_product (job_id, uri, product_name) VALUES (%s, %s, %s) RETURNING source_product_id"
                    product_to_insert = (jobid, row[1], row[0])
                    cursor.execute(insert_product, product_to_insert)
                    conn.commit()
                    prod_index = cursor.fetchone()[0]
                    product_list.append(prod_index)

        # check if there are more than 100 products and retrieve them in case
        while num_lines > 99:
            page += 1
            url = f'./dhusget.sh -d {hub} -u {usr_name} -p {usr_psw} -m {mission} -S {start_date_list[i]} -E {stop_date_list[i]} -c {shapeBoundaries[0]},{shapeBoundaries[1]}:{shapeBoundaries[2]},{shapeBoundaries[3]} -T {data_type} -l 100 -P {page}'  # -q {query_result} -C {products_list}'
            os.system(url)
            logger.info('Query done! Writing data on db ... ')
            num_lines = sum(1 for line in open('products-list.csv'))
            logger.info('Found: ' + str(num_lines) + ' on page ' + str(page))
            # update db with entries if products have been found
            if num_lines != 0:
                insert_jobs = "INSERT INTO job (order_id, start_date, end_date) VALUES (%s, %s, %s) RETURNING job_id"
                records_to_insert = (orderid, start_date_list[i], stop_date_list[i])
                cursor.execute(insert_jobs, records_to_insert)
                conn.commit()
                jobid = cursor.fetchone()[0]
                # read products from query resulting file and put them in db
                with open('products-list.csv', 'r') as prodlis:
                    csv_reader = csv.reader(prodlis, delimiter=',')
                    for row in csv_reader:
                        insert_product = "INSERT INTO source_product (job_id, uri, product_name) VALUES (%s, %s, %s) RETURNING source_product_id"
                        product_to_insert = (jobid, row[1], row[0])
                        cursor.execute(insert_product, product_to_insert)
                        conn.commit()
                        prod_index = cursor.fetchone()[0]
                        product_list.append(prod_index)

    # remove files in folder
    os.system("rm product*")
    os.system("rm *.xml")

    return product_list


@task
def downloadSourceProduct(sourceproductid):
    # logging connection
    logger = prefect.context.get("logger")
    logger.info(" sourceProduct:" + str(sourceproductid))

    # establish connection with db
    conn = psycopg2.connect(
        host='localhost',
        database='nuts_db',
        user='nuts_user',
        password='nuts_password',
        port=5432
    )
    cursor = conn.cursor()

    # get the source product id from source product list
    order_request = "SELECT * FROM source_product WHERE source_product_id = '" + str(sourceproductid) + "'"
    cursor.execute(order_request)
    product = cursor.fetchone()
    jobid = product[1]
    logger.info("Checking for product: " + str(product[3]))

    # download products from catalogue
    usr_name = 'gianpieroizzo'
    usr_psw = 'passwordXmonica'
    file_name = f'{product[3]}.zip'
    products_folder = f'{os.getcwd()}/cache_folder/'

    # if file is present in folder do not launch the wget command else do it
    if os.path.isfile(os.path.join(products_folder, file_name)):
        logger.info("Product " + str(product[3]) + "  already downloaded!")
        res_download = 0
        download_status = 'downloaded'
    else:
        logger.info("Downloading: " + str(product[3]))
        maxretry = 100
        cmd = f'wget -nc --content-disposition --continue --user={usr_name} --password={usr_psw} "{product[2]}/\$value" 2> wget_response_{sourceproductid}_job_{jobid}.txt'
        res_download = 1
        i_retry = 1
        time_to_sleep = 0
        download_status = 'not downloaded'
        # while loop necessary until only 4 products per time can be downloaded
        while res_download > 0:
            os.system(cmd)
            res_download = os.WEXITSTATUS(res_download)
            logger.info("In the while loop")
            with open(f"wget_response_{sourceproductid}_job_{jobid}.txt", "r") as res:
                if '202 Accepted' in res.read():
                    logger.info("202 Accepted")
                    time_to_sleep = 600
                    res_download = 1
                    download_status = 'failed'
                elif '403 Forbidden' in res.read():
                    logger.info("403 Forbidden")
                    time_to_sleep = 60
                    res_download = 1
                    download_status = 'failed'
                elif 'Saving to:' in res.read():
                    logger.info("200 Saving")
                    res_download = 0
                    time_to_sleep = 0
                    logger.info("Product " + str(product[2]) + " downloaded!")
                    download_status = 'downloaded'

            logger.info("While-loop: " + str(cmd) + " - result: " + str(res_download) + " - retry num=" + str(i_retry))
            i_retry += 1
            sleep(time_to_sleep)  # wait before retry download
            if i_retry > maxretry:  # security stop: if max number of iter reached
                break
        logger.info("Command: " + str(cmd) + " - result: " + str(res_download))

    # define download STATUS: downloaded or failed
    if res_download == 0:
        # move zipped products in cache folder to clean up directory
        cmd = f'mv {file_name} {products_folder}'
        os.system(cmd)
        cmd = f'rm wget_response_{sourceproductid}_job_{jobid}.txt'
        os.system(cmd)
    else:
        logger.info("Problem downloading product " + str(sourceproductid))

    # update db with download status
    insert_bounds = "UPDATE source_product SET status = %s WHERE source_product_id = %s"
    cursor.execute(insert_bounds, (download_status, sourceproductid))
    conn.commit()

    return sourceproductid

