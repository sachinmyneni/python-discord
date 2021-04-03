import os
import logging
import subprocess
import argparse
from sys import platform,version,exit,argv
from typing import Optional
import socket
from time import localtime, strftime
import setup_resources
import tasks
import pyos_utils
from optiutils import askdb
from client_maker import make_p4client

#unknown: Optional[str]=None,
def main(args: dict, unknown: list):
  loglevel=logging.DEBUG if args.debug else logging.WARNING
  logging.basicConfig(level=loglevel, format='[{asctime}:{filename}:{lineno}] {msg}',style="{")
  logging.critical(f"REAL logging level: {logging.getLevelName(logging.getLogger(None).level)}")
  logging.debug(f"In {__name__}")
  logging.critical(f"Logging level = {logging.getLevelName(loglevel)}\nUse --debug for more info.")
  logging.critical(f"REALLY REAL logging level: {logging.getLevelName(logging.getLogger(None).level)}")

  if 'unknown' in locals():
    if len(unknown) > 0:
      logging.debug(f"Extra option: {unknown}")
      unknown_opt = " ".join(unknown) if isinstance(unknown,list) else unknown

  if 'unknown_opt' in locals():  # will always have 'option' in current buildweb+jenkins; but not in my local tests.
    if '#' in unknown_opt:
      for taskname in unknown_opt.split('#'):
        if args.task in taskname:
          logging.debug(f"Found the task: {taskname}")
          action = taskname.split('@')[1].strip()
          logging.debug(f"Requested action: {action}")
          if 'skip' in action:
            print(f"Skipping {args.task}")
            exit(0)
          else:
            subacts = action.split(' ')
            logging.warning(f"Additional options: {subacts}")
    else:
      subacts = unknown_opt
      logging.warning(f"Additional user options: {subacts}")
  else:
    logging.debug(f"\'Standard build\' requested")


  base_path = os.getcwd()
  print(f" {strftime('%Y-%m-%d %H:%M:%S', localtime())}: Begin setup")
  print(f"Python Version: {version}")
  print(f"Current Location: {base_path}")

  TASKS_FILE = 'tasks.yml' if not args.taskfile else args.taskfile
  RESOURCES_FILE = 'resources.yml' if not args.resourcefile else args.resourcefile

  logging.info("Read TASKS and RESOURCES")
  opts = None
  if args.branch:
    this_branch = args.branch
    opts = {'branch':this_branch}

  this_job = tasks.Job(TASKS_FILE, opts)

  task_list = this_job.create_job(opts)
  node_list = setup_resources.create_node_list(RESOURCES_FILE,opts)

  # Label list: List of dicts. Keys are label names. Values are list of nodes
  label_list = []
  for node in node_list:
    for label in node.label_membership:
      if label != node.name:
        if any(label in ll for ll in label_list):
          try:
            num_index = next((i for i,d in enumerate(label_list) if label in d),None)
            label_list[num_index][label].append(node.name)
          except IndexError:
            logging.error(f"No index found when expected for {node.name}")
        else:
            label_list.append({label:[node.name]})


  if args.jobid:
    print(f"JOBID: {args.jobid}")
    db = askdb.Database()
    clnum = db.getcl_from_jobid(args.jobid)['clnum'] if not args.changelist else args.changelist
    print(f"CHANGELIST: {clnum}")
  else:
    clnum = args.changelist if args.changelist else None

  if args.task:
    # find the task in this_job

    assert args.task in [item.__name__ for item in task_list], f"{args.task} not in the list of tasks in {TASKS_FILE}"
    
    this_task = next(filter(lambda x: x.__name__ == args.task,task_list), None)
    if this_task:
        logging.info(f"Found task to do: {this_task.__name__}")
    else:
        logging.error(f"{this_task.__name__} was not found!")
        logging.error(f"This should not have happened if {this_task.__name__} was defined in {TASKS_FILE} ")
        raise ValueError
      # find the resource for this task.
    all_nodes_names = [node_name for node_name in [t.name for t in node_list]]
    if this_task.get_runs_on() not in all_nodes_names and not any(this_task.get_runs_on() in ll for ll in label_list):
        logging.error(f"The node/Label required for {this_task.__name__}: '{this_task.get_runs_on()}' is not in the resource list")
        raise ValueError
    # The following has to be updated to return alternate node if get_runs_on() returns a label
    this_node = next(filter(lambda x: x.name == this_task.get_runs_on(),node_list), None)

    if not this_node: # perhaps a label?
      this_node_group = list(next(filter(lambda x: list(x.keys())[0] == this_task.get_runs_on(),label_list), None))[0]

    # Am I being run on the appropriate resource?
    if socket.gethostname() not in [this_task.get_runs_on()]:
      if not any(socket.gethostname() in hostlist for hostlist in next((list(d.values()) for i,d in enumerate(label_list) if this_node_group in d),None)):
        logging.error(f"{this_task.__name__} is not being run on the expected node {this_task.get_runs_on()} but on {socket.gethostname()}")
        print(f"{this_task.__name__} is not being run on the expected node {this_task.get_runs_on()} but on {socket.gethostname()}")
        raise ValueError
      else:
        #  This node is part of the label. Accept and run on it.
        logging.info(f"Executing on {socket.gethostname()} which has been accepted as part of the label {this_node_group}")
        this_node = next(filter(lambda x: x.name == socket.gethostname() ,node_list),None)


    # set up the resource (if any) and...
    # Run the task
    logging.debug("\n\nRun the task now..")
    if this_node.setup_commands:
      logging.debug(f"Setup commands: ")
      for c in this_node.setup_commands:
          logging.debug("\t"+c)  
      logging.debug(f"Run Commands: ")

    if isinstance(this_task.run(),str):  # Make single command into a list of one command. 
      onecmd = this_task.run()
      this_task.update_command(onecmd,[onecmd])

    for r in this_task.run():
      if not isinstance(this_task,tasks.Qa):  # QA always sync's to the latest.
        if r == 'p4 sync' and 'clnum' in locals() and clnum is not None:
          new_cmd = r + " @" + str(clnum)
          this_task.update_command(r,new_cmd)
      if r.startswith('perl ./qa_script') and 'clnum' in locals() and clnum is not None:
        new_cmd = r.replace('EXECUTABLE_NAME',this_task.exe_loc+"/"+str(clnum)+"/"+this_task.exe_name)
        opt_for_db = "--nightlyqa=" + str(clnum) + " " + "--branch=OS_" + (this_task.branch).upper() if isinstance(this_task.branch,str) else (this_task.branch)
        if 'subacts' in locals():
          if len(subacts) > 1: # likely a verifyQA run.
            opt_for_db = " ".join(subacts[1:])

        assert 'opt_for_db' in locals()
        
        this_task.update_command(r,new_cmd + " " + opt_for_db)  # opt_for_db should be absent for verify-QA runs.

        logging.debug("\t"+" ".join(this_task.run()))

    if this_task.pre_task_env:
      if args.dryrun:
        print(f"Setting environment: {this_task.pre_task_env}")
      pyos_utils.set_this_env(this_task.pre_task_env)

    if this_task.type == 'qa' or not this_node.setup_commands:
      commands_to_run = this_task.run()
    else:
      commands_to_run = this_node.setup_commands+this_task.run()

    #  User defined args to be appended only to build commands.
    #  And subacts is a str only if it is not standard buildweb task
    #  This could be improved somehow...
    if 'subacts' in locals() and isinstance(subacts,str):
      commands_to_run =  list(map(lambda x: x+" "+subacts if x.startswith(('make','devenv','wslo.bat')) else x, commands_to_run))

    
    logging.debug(f"All commands: {commands_to_run}")
    if args.dryrun:
      print(f"DRYRUN: All commands: {commands_to_run}\n")

    #Pre task copy
    if args.copy_exe:
      if this_task.copy_from is not None:
        this_task.update_path(this_task.copy_from,"src",clnum)
        if args.dryrun:
          print(f"DRYRUN: Copy {this_task.copy_from.get('artifacts') or this_task.artifacts} from: {this_task.copy_from['src']}\n")  
        else:
          if 'artifacts' in this_task.copy_from:
            pyos_utils.copyosfile(this_task.copy_from)
          else:
            pyos_utils.copyosfile(this_task.copy_from,artifact=this_task.artifacts)
      
    #Task
    path = this_node.workarea if not isinstance(this_task,tasks.Qa) or this_node.qa_workarea is None else this_node.qa_workarea
    try_again = True
    if args.dryrun:
      print(f"DRYRUN: Executing: {commands_to_run} at {path}\n")
    else:  
      while try_again:
        try:
          pyos_utils.runner(commands_to_run,path)
          try_again = False
        except NotADirectoryError as nade:
          logging.error(f"{path} is not a directory on {socket.gethostname()}.")
          logging.error("--Begin Stack--")
          print(nade)
          logging.error("--End Stack--")
          logging.info("Creating workarea")
          client_root = this_node.workarea.split(this_node.client,1)[0]
          try_again = make_p4client.make_client(this_node.client,this_node.client_template,client_root)
      else:
        try_again = False

    #Post task copy
    if this_task.copy_to is not None:
      if not args.skip_copy:
        this_task.update_path(this_task.copy_to,"dest",clnum)
        if args.dryrun:
          print(f"DRYRUN: Copy {this_task.copy_to.get('artifacts') or this_task.artifacts} to: {this_task.copy_to['dest']}\n") 
        else:
          if 'artifacts' in this_task.copy_to:
            pyos_utils.copyosfile(this_task.copy_to,path=path)
          else:
            pyos_utils.copyosfile(this_task.copy_to,artifact=this_task.artifacts,path=path)
    if isinstance(this_task,tasks.Qa):
      if args.dryrun:
        print(f"DRYRUN: Copy: {this_task.qa_artifacts} from {path} to {base_path}")
      else:
        logging.debug(f"Copying {this_task.qa_artifacts} from {path} to {base_path}")
        pyos_utils.copyosfilelist(this_task.qa_artifacts,base_path,srcpath=path)

  del(this_task)

if __name__ == "__main__":
  print(__name__)
  parser = argparse.ArgumentParser(description="Parent script to run tasks on various systems.")
  parser.add_argument("-b","--branch", help="Branch to do the build of")
  parser.add_argument("-c","--check", help="Check configuration files for errors", action='store_true')
  parser.add_argument("-d","--deps", help="Show dependencies between tasks", action='store_true')
  parser.add_argument("-j","--jobid", help="Buildweb JOBID")
  parser.add_argument("-cl","--changelist", help="Changelist to sync to for tasks of this job")
  parser.add_argument("-t","--task",help="Task to run")
  parser.add_argument("-dbg","--debug", help="print more debug information", action='store_true')
  parser.add_argument("--skip_copy",help="[Build] Skip copying of executable after build", action='store_true')
  parser.add_argument("--copy_exe",help="[Qa] Copy executable to the current machine before the QA instead of using it remotely", action='store_true')
  parser.add_argument("--timeout",help="Timeout in seconds")
  parser.add_argument("-f","--force",help="Force the operation: build,copy etc")
  parser.add_argument("--dryrun",help="Just print out what would be done and exit",action='store_true')
  parser.add_argument("-tf","--taskfile",help="Full path to the YAML File with tasks")
  parser.add_argument("-rf","--resourcefile",help="Full path to the YAML File with resources")

  args, unknown = parser.parse_known_args()

  exit(main(args,unknown))
