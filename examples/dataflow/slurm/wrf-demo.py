from dagon import Workflow
from dagon.task import DagonTask, TaskType
import json
import os.path
import time
from time import sleep


"""
SLURM
There are a tot of file that will be used as description of data for that day, for the coherence of the data
There are 3 domains, each domain contains a file for each hour, so:
-d01 for the 00:00, d02 for the 00:00 and d03 for the 00:00;
-d01 for the 01:00, d02 for the 01:00, d03 for the 01:00
...
The publish will take for each domain the current hour, the previous hour and the one of the 00:00 of that day

Step 1: Copy the files of wrf of that hour to the scratch directory of the task A (so number of domains file 
will be copied in the scratch directory).
Step 2: let the publish begin the streaming on the files that has been copied (so discarding the first 3 hours, 
and taking the current hour, the previous hour and the 00:00 hour (automatically)). The publish has to be modifed 
and executed with the specification of the number of hours per domain, the number of domain. So there will be a .c 
file, that will calculate the number of expected files, and execute the publish.
The publish will be executed for each domain, and it is necessary to pass to it the 3 files of that domain of the 
related hours (the current, previous and 00:00).
The publish will take the files in input from the scratch directory of wrf (Task A) and will publish after the execution
in its scratch directory (Task B -> so 2 tasks).


The .c file will have the same structure of the B.c but before open the file in the ouptut directory, we will call the publish.
So we will only setup the space for the name of the file, and the name of the file , and subsequently pass to the publish as
a parameter.
"""


# Check if this is the main
if __name__ == '__main__':

    # Create the orchestration workflow
    workflow = Workflow("DataFlow-Demo-Slurm")

    # Set the dry, if it is false the execution will be really executed
    workflow.set_dry(False)
    BASE_PATH = os.getenv("CAPIO_BASE_PATH", "/default/path")
    workflow.logger.debug(workflow.get_capio_dir_base())

    # The task a
    cmdWrf = "{}/wrf {} workflow:///makeInputNameList_{}/namelist.input workflow:///real/wrfbdy\* workflow:///real/wrfinput\* {}".format(
        command_dir_base_wrf, i_date, str(day), restartFile)
    taskWrf = DagonTask(TaskType.SLURM, "wrf_" + str(day), cmdWrf, partition="hxcpu", memory=128000, time="2:30:00",
                        nodes=6, ntasks_per_node=48)

    # The task b
    cmdPublishWrf = "{}/publishWrfOutput.dist {} {} {} workflow:///wrf_{}/{} workflow:///wrf_{}/{} workflow:///wrf_{}/{}".format(
        command_dir_base_wrf, i_date, wrf_model, skip_hours_wrf, str(day), curr_file, str(day), prev_file, str(day),
        zero_file)
    taskPublishWrf = DagonTask(TaskType.SLURM, "publishWrfOutput_{}_{}".format(str(day), curr_file), cmdPublishWrf,
                               partition="xxcpu", ntasks=1, memory=8192)


    # add tasks to the workflow
    workflow.add_task(taskA)
    workflow.add_task(taskB)

    workflow.make_dependencies()

    if workflow.get_enable_capio_execution():
        workflow.create_scratch_directory_names_tasks_capio()

        jsonCapioWorkflow = workflow.as_json_capio()
        with open('pipeline-demo-capio.json', 'w') as outfile:
            stringWorkflow = json.dumps(jsonCapioWorkflow, sort_keys=False, indent=2)
            outfile.write(stringWorkflow)

        workflow.set_capio_server_path("/home/capio/build/src/server")
        workflow.set_capio_libcapioposix_path("/home/capio/build/src/posix")
        workflow.set_capio_libsyscall_intercept_path("/home/opt/capio-v2/lib")
        workflow.run_capio_server()
        sleep(1)
        workflow.is_server_capio_running()

        workflow.create_scratch_directory_tasks_capio()
        workflow.wait_for_all_dependency_directories()

        workflow.generate_script_wrf()

        workflow.remove_all_task_reference_workflow()