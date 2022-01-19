from prefect import Flow, Parameter, flatten
from prefect.executors import LocalDaskExecutor

from nuts import common
from nuts import dem

with Flow("DEM_workflow") as flow:
    orderid = Parameter("orderid")
    shapeBoundaries = common.retrieveShape(orderid)
    downloaded_products = dem.downloadDEMproduct(orderid, shapeBoundaries)
    jobs = common.organizeJobs(downloaded_products, orderid)
    result = dem.cropDEMproducts.map(flatten(jobs))
    common.notifyResult(result)

flow.executor = LocalDaskExecutor()
flow.register(project_name="NUTS")
