from prefect.deployments import Deployment
from etl_web_to_gcs import web_to_gcs
from prefect.filesystems import GitHub

github_block = GitHub.load("de-zoomcamp-github")

github_dep = Deployment.build_from_flow(
    flow=web_to_gcs,
    name="github-flow",
    storage=github_block,
    parameters={'month': 11, 'year': 2020,'color': 'green'}
)


if __name__ == "__main__":
    github_dep.apply()