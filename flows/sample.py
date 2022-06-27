import prefect
from prefect import task, Flow
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.storage import GitHub
from prefect.triggers import all_finished
from prefect.run_configs import LocalRun
import pygit2

DBT_PROJECT = "demo"
STORAGE = GitHub(
    repo="klxsxian/demo",
    path=f"flows/sample.py",
    access_token_secret="GITHUB_ACCESS_TOKEN",
)

dbt = DbtShellTask(
    return_all=True,
    profile_name="profiles",
    profiles_dir="ci_profiles/",
    environment="dev",
    overwrite_profiles=True,
    log_stdout=True,
    helper_script=f"cd {DBT_PROJECT} && dbt deps",
    log_stderr=True,
    dbt_kwargs={
        "type": "postgres",
        "host": "124.221.124.73",
        "port": 15432,
        "dbname": "postgres",
        "schema": "schema_dbt",
        "threads": 4,
        "client_session_keep_alive": False,
    },
)

@task(trigger=all_finished)
def print_dbt_output(output):
    logger = prefect.context.get("logger")
    for line in output:
        logger.info(line)

@task(name="Clone DBT repo")
def pull_dbt_repo():
    pygit2.clone_repository(url="https://github.com/klxsxian/demo",
                            path=DBT_PROJECT, checkout_branch=None)

with Flow("sample", run_config=LocalRun(labels=["myAgent"],env={"GITHUB_ACCESS_TOKEN": "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"})) as flow:
    pull_task = pull_dbt_repo()
    dbt_run = dbt(command="dbt run", task_args={"name": "DBT Run"})
    dbt_run_out = print_dbt_output(dbt_run)