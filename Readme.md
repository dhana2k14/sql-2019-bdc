# Schedule a Notebook to run on SQL 2019 BDC

Below are the steps and the noteboooks to execute in a sequence for scheduling your notebook to run via cron job based on `azdata notebook run` method. This is a way around and will be removed in future once the GUI based feature for scheduling notebook run via `SQL Agent` is ready by Microsoft. 

**Note:** As of writing this only the notebooks written using SQL and Scala as kernels are supported via `azdata notebook run` method and other notebooks written using any other kernels like PySpark or SparkR are not be supported as this feature is currently in development with Microsoft product team. 

**Update from Microsoft:** Scheduling a notebook with PySpark kernel will likely to available with CU8 or CU9 versions of SQL 2019 BDC.   

**Important Note:** Since SQL kernel based Notebooks are executed using Powershell's `Invoke-SqlNotebook` method, it is required that **all code cells should have one line for each SQL procedure that we write**. This is an expected behaviour in powershell or related bug.

```
For example:
This will work properly:
select * from sys.databases where database_id = 1
But this will not give the required results and executed only the first line and ignore the next line:
select * from sys.databases 
where database_id = 1
```

## What is `azdata`? 

Azure Data CLI (azdata) is a command-line utility tool written in Python to bootstrap and manage SQL Server Big Data Clusters data services via REST API. For more details refer to [azdata](https://docs.microsoft.com/en-us/sql/azdata/install/deploy-install-azdata?view=sql-server-ver15)

## Table of Contents

|Notebook|Description|
|---|---|
|[Create app](opr0001-create-app-deploy.ipynb)|Create and deploy an app in Kubernetes Pod|
|[Run app](opr002-run-app-deploy.ipynb)|Run the deployed app|
|[Create job](opr003-create-cronjob.ipynb)|Create a cron job that runs the app|
|[Run Notebook](run001-run-notebook.ipynb)|A notebook used in setting up the environment for the target notebooks to run|
|[Target Notebook](01-simple-sql-notebook)|A target notebook to schdule|

## Prepare & Run Notebooks
The following notebooks should be executed in a sequence once configured as illustrated in the steps below. 

### Create App and Deploy
**Description:** This notebook will create an app which in turns executes the target notebook and deploy it in Kubernetes pod. 

**Notebooks to be used:** `opr0001-create-app-deploy_copy.ipynb`, `run001-run-notebook.ipynb`, `target_notebook.ipynb`

1. Create a folder anywhere in your local machine with a name of your choice. Copy your target `.ipynb` notebook which you need to schedule and `run001-run-notebook.ipynb` to this newly created folder. Note the parent directory of the newly created folder. 
2. Open `opr0001-create-app-deploy_copy.ipynb` in Azure Data Studio(ADS) and do the following.

    2.1. Change the ``notebooks`` variable in **Parameters** cell of the notebook. 
    
    ```
    os.path.join('parent-directory-of-the-folder', 'folder-name', 'target-notebook-name-with-extension')
    ```

    2.2. Locate **Copy notebook files to app-deploy staging folder** code cell in the notebook and edit ``additional_notebooks`` variable. 

    ```
    os.path.join('parent-directory-of-the-folder', 'folder-name', 'run001-run-notebook.ipynb')
    ```
3. Save the `opr0001-create-app-deploy_copy.ipynb`. 
4. Open `run001-run-notebook.ipynb` in ADS. 

    4.1. Set the following variables with repsective values in the first code cell of the Notebook. 
    |variable|value|
    |---|---|
    |sql_master_pool_username|bdcadmin|
    |sql_master_pool_password|Admin@@123|
    |knox_username|bdcadmin|
    |knox_password|Admin@@123|

    4.2. Change ``notebook_path`` variable in teh code cell as below.
    ```
    notebook_path = os.path.join(os.getcwd(), "run001-run-notebook.ipynb")
    ```
    
    4.3. Locate **Run a SQL Kernel notebook** text cell in the Notebook and change the end point in the code cell as follows. The end point can be obtained from runnning `azdata bdc endpoint list` in command prompt or in ADS terminal after running `azdata login` with necessary inputs.

    From:
    ```
    sql_server_master_endpoint = "master-p-svc,1433"
    ```

    To:
    ```
    sql_server_master_endpoint = "172.31.61.92,31433"
    ```

    4.4. Locate **Inject authentication for PySpark/Scala kernel based notebooks** text cell and change the code as follows.

    From:
    ```
    if inside_kubernetes_cluster:
        set_endpoint_cmd = "%_do_not_call_change_endpoint --server=https://gateway-svc:8443/gateway/default/livy/v1 "
    ``` 
    To:
    ```
    if inside_kubernetes_cluster:
        set_endpoint_cmd = "%_do_not_call_change_endpoint --server=https://172.31.61.93:30443/gateway/default/livy/v1"
    ```

    4.5. Locate **Run the Notebook** text cell. Add a new code cell below to it. Paste the following the code into the newly added code cell. 
    ```
    import os,  json, datetime, shlex, subprocess, glob, base64
    from subprocess import PIPE, Popen

    os.environ["AZDATA_USERNAME"] = "bdcadmin"
    os.environ["AZDATA_PASSWORD"] = "Admin@@123"
    os.environ["ACCEPT_EULA"] = "yes"

    run(f'azdata login --namespace mssql-cluster --endpoint https://172.31.61.92:30080')
    ```
    
    4.6. Remove the code cells under both **Record the results** and **Run Expert Rules** text cells in the notebook as these code cells are not required. 
5. Save `run001-run-notebook.ipynb`. 
6. Open `opr0001-create-app-deploy_copy.ipynb` and click on **Run all**. This will execute each code cells of the notebook one by one and stops at the code cell if it encounters any error. We could also see the output printed below of each code cells.  

### Run app
**Description:** This notebook will run the deployed app. 
**Notebooks to be used:** `opr002-run-app-deploy_copy.ipynb`

1. Open `opr002-run-app-deploy_copy.ipynb` in ADS. 
2. Make sure that the value of **app_name** variable is set and click **Run all**.  
3. Verify the output of the text cell **Verify the app-deploy runs using azdata** and check if any errors. If it errors out, you could view one of the notebooks under the **changedFiles** variable in the output to debug the error. 
4. If no errors then proceed with executing `create-cronjob.ipynb`, otherwise follow the next step to debug the error. 

## How to debug if the Run app Notebook fails with an error?

As aforementioned in step3, while `opr002-run-app-deploy_copy.ipynb` notebook is being executed, it will save both the target notebook and the run001-run-notebook with the output of each code cells in the Kubernetes Cluster machine, which we will copy over to our local PC and view it to locate which particular code cell causing an error. Below are the steps involved in doing Kubectl commands that copy over the notebooks to local PC. 

1. Open command prompt or terminal in ADS, type `kubectl get pods -n mssql-cluster` and hit enter. It will list out all the pods running currently. 
2. Locate the pod name under **NAME** column and type `kubectl exec -it <name-of-pod> -n mssql-cluster bash` and hit enter. 
3. Type `cd /var/opt/mlserver/workdir/` and hit enter. Further type `cd <folder-name-at-the-location>` and hit enter.
4. Type `pwd` and hit enter. This will print the current working directory path and copy it.
5. Type `exit` and hit enter. 
6. Type `kubectl exec -it <name-of-pod> -n mssql-cluster cat <path-of-the-notebook>/<full-name-of-notebook> > <local-destination-path>`
7. Now open the notebook which we saved in local drive and check if there are any errors in any of code cells output pane. 
8. Fix the error in the original versions of notebooks and repeat executing the notebooks of `opr0001-create-app-deploy_copy.ipynb` and `opr002-run-app-deploy_copy.ipynb`. 
9. Repeat steps 1 to 7 as above for debugging further errors if any. 

### Create Cron Job
**Description:** This notebook will run deployed app under a schedule as a cron job. 
**Notebooks to be used:** `create-cronjob.ipynb`

1. Open `create-cronjob.ipynb` in ADS.
2. Make sure that the value of **app_name** variable is set and click **Run all**. 
3. Edit **Schedule** variable according to your requirments. 
4. If no errors, then the deployed app is now scheduled based on the schedule above as a cron job. 
5. We can list all the scheduled cron jobs by running `kubectl get cronjob -n mssql-cluster`  