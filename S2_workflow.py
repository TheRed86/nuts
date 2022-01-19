from prefect import Flow, Parameter, flatten
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import DockerRun
from prefect.storage import GitHub

from nuts import common, dhus, s2

with Flow("S2_workflow") as flow:
    orderid = Parameter("orderid")
    shapeBoundaries = common.retrieveShape(orderid)
    source_products = dhus.queryCatalog(orderid, shapeBoundaries)
    downloaded_products = dhus.downloadSourceProduct.map(flatten(source_products))
    jobs = common.organizeJobs(downloaded_products, orderid)
    result = s2.mergeProducts.map(flatten(jobs))
    common.notifyResult(result)

flow.run_config = DockerRun(
   image="aliaspace/nuts:1.1"
)
flow.executor = LocalDaskExecutor()

flow.storage = GitHub(
    repo="TheRed86/nuts",                           # name of repo
    path="S2_workflow.py",                   # location of flow file in repo
    # access_token_secret="GITHUB_ACCESS_TOKEN",  # name of personal access token secret
    )
flow.register(project_name="NUTS")
