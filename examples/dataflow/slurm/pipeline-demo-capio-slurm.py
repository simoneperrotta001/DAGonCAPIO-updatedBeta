from dagon import Workflow
from dagon.task import DagonTask, TaskType
import json
import os.path
import time
from time import sleep

# Check if this is the main
if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo-Slurm")

    # Set the dry, if it is false the execution will be really executed
    workflow.set_dry(False)
    BASE_PATH = os.getenv("CAPIO_BASE_PATH", "/default/path")
    workflow.logger.debug(workflow.get_capio_dir_base())

    # The task a
    taskA = DagonTask(TaskType.SLURM, "A",  f"{workflow.get_capio_dir_base()}/A",
                      partition="gpu", ntasks=1, memory=8192)

    # The task b
    taskB = DagonTask(TaskType.SLURM, "B", f"{workflow.get_capio_dir_base()}/B workflow:///A",
                      partition="gpu", ntasks=1, memory=8192)

    # The task c
    taskC = DagonTask(TaskType.SLURM, "C", f"{workflow.get_capio_dir_base()}/C workflow:///B",
                      partition="gpu", ntasks=1, memory=8192)

    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)
    workflow.add_task(taskC)

    workflow.make_dependencies()

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
    sleep(1)
    workflow.generate_script_pipeline()

    workflow.remove_all_task_reference_workflow()
