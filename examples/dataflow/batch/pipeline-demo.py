import json
import time
import os
from time import sleep

from dagon import Workflow
from dagon.task import DagonTask, TaskType
from dotenv import load_dotenv

#load_dotenv("capio.env")

#TODO: view if there is any possibility to generalize much more the logic even in these workflow python file, distinguishing automatically the execution with or without CAPIO

# Check if this is the main
if __name__ == '__main__':
    # Create the orchestration workflow
    workflow = Workflow("Pipeline-Demo")

    # Set the dry, if it is false the execution will be really executed
    workflow.set_dry(False)
    BASE_PATH = os.getenv("CAPIO_BASE_PATH", "/default/path")
    workflow.logger.debug(workflow.get_capio_dir_base())

    # The task a
    taskA = DagonTask(TaskType.BATCH, "A", f"{workflow.get_capio_dir_base()}/A CAPIO")

    # The task b
    #taskB = DagonTask(TaskType.BATCH, "B", f"{BASE_PATH}/B workflow:///A")
    taskB = DagonTask(TaskType.BATCH, "B", f"{workflow.get_capio_dir_base()}/B workflow:///A CAPIO")

    taskC = DagonTask(TaskType.BATCH, "C", f"{workflow.get_capio_dir_base()}/C workflow:///B")


    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)

    workflow.make_dependencies()
    #for task in workflow.tasks:
        #task.create_working_dir()
    if workflow.get_enable_capio_execution():
        workflow.create_scratch_directory_names_tasks_capio()

        jsonCapioWorkflow = workflow.as_json_capio()
        with open('pipeline-demo-capio.json', 'w') as outfile:
            stringWorkflow = json.dumps(jsonCapioWorkflow, sort_keys=False, indent=2)
            outfile.write(stringWorkflow)

        workflow.set_capio_server_path("/home/sperrotta/capio/build/src/server")
        workflow.set_capio_libcapioposix_path("/home/sperrotta/capio/build/src/posix")
        workflow.set_capio_libsyscall_intercept_path("/home/sperrotta/opt/capio-v2/lib")
        workflow.run_capio_server()
        sleep(1)
        workflow.is_server_capio_running()

        workflow.create_scratch_directory_tasks_capio()
        workflow.wait_for_all_dependency_directories()

        jsonWorkflow = workflow.as_json()
        with open('pipeline-demo.json', 'w') as outfile:
            stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
            outfile.write(stringWorkflow)


        workflow.generate_script_pipeline()
        workflow.remove_all_task_reference_workflow()