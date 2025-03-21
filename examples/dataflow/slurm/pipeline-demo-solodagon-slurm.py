from dagon import Workflow
from dagon.task import DagonTask, TaskType
import json
import os.path
import time

# Check if this is the main
if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo-Slurm")

    # Set the dry
    workflow.set_dry(False)

    # The task a
    taskA = DagonTask(TaskType.SLURM, "A", f"{workflow.get_capio_dir_base()}/A",
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

    jsonWorkflow = workflow.as_json()
    with open('dataflow-demo-slurm.json', 'w') as outfile:
        stringWorkflow = json.dumps(jsonWorkflow, sort_keys=True, indent=2)
        outfile.write(stringWorkflow)

    # create the working directory of each task

    # run the workflow
    workflow.run()
