import inspect
import shutil
import subprocess
from datetime import datetime
from os import makedirs

from .taskman import Taskman
from .job import JobStatus, Job
from . import CKPT_FOLDER, HOMEDIR, DB_STARTED_TASKS


def handle_command(cmd_str):
    tokens = cmd_str.split(' ', maxsplit=1)
    cmd_name = tokens[0]
    if cmd_name == '':
        return
    if len(tokens) == 1:
        cmds[cmd_name]()
    else:
        cmd_args = ' '.join(tokens[1:])
        cmds[cmd_name](*[arg.strip() for arg in cmd_args.split(';')])


def show_commands():
    print('-------------------')
    print('Available commands:')
    for name, fn in sorted(cmds.items(), key=lambda x: x[0]):
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        print(name, ':', '; '.join([str(p) for p in params]))


def _match(pattern, name):
    if pattern.endswith('*'):
        return name.startswith(pattern[:-1])
    else:
        return name == pattern


def submit(template_file, args_str, task_name):
    job = Taskman.create_task(template_file, args_str, task_name)
    Taskman.submit(job)


def fromckpt(template_file, args_str, task_name, ckpt_file):
    job = Taskman.create_task(template_file, args_str, task_name)
    print('Moving checkpoint...')
    job_dir = CKPT_FOLDER + '/' + job.name + '/' + job.task_id
    makedirs(job_dir)
    shutil.move(HOMEDIR + '/' + ckpt_file, job_dir)
    Taskman.submit(job)


def multi_sub():
    print('Enter multiple submission lines. Add an empty line to end.')
    print()
    a = []
    while True:
        i = input()
        if i == '':
            break
        a.append(i)
    print('Tasks to submit:')
    for i in a:
        print(i)
    print()
    r = input('Submit? (y/n)')
    if r == 'y':
        for i in a:
            submit(*i.split(';'))


def continu(task_name):
    for task_id, job in Taskman.jobs.items():
        if (job.status in [JobStatus.Dead, JobStatus.Lost] or job.status == JobStatus.Finished
                and job.finish_msg == 'cancel') and _match(task_name, job.name):
            Taskman.submit(job)


def cancel(task_name):
    for task_id, job in Taskman.jobs.items():
        if job.status.cancellable and _match(task_name, job.name):
            Taskman.cancel(task_id)


def copy(task_name):
    submitted = set()
    for task_id, job in Taskman.jobs.items():
        if job.name not in submitted and _match(task_name, job.name):
            job = Taskman.create_task(job.template_file, job.args_str, job.name)
            Taskman.submit(job)
            submitted.add(job.name)


def show(task_name):
    print()
    for task_id, job in Taskman.jobs.items():
        if _match(task_name, job.name):
            print('\033[1m' + job.name + '\033[0m :', job.args_str)
            print('\033[30;44m' + ' ' * 40 + '\rOutput\033[0m')
            for l in Taskman.get_log(job)[-10:]:
                print(l.strip())
            print('\033[30;44m' + ' ' * 40 + '\rError\033[0m')
            for l in Taskman.get_log(job, error_log=True)[-30:]:
                print(l.strip())
            print('\033[30;44m' + ' ' * 40 + '\033[0m')
            print()
    input('Press any key...')


def pack(task_name):
    checkpoint_paths = []
    for task_id, job in Taskman.jobs.items():
        if job.status == JobStatus.Finished and _match(task_name, job.name):
            checkpoint_paths.append(job.name + '/' + job.task_id)
    # Call pack.sh
    subprocess.Popen([HOMEDIR + '/taskman/pack.sh'] + checkpoint_paths)


def results(task_name):
    files = []
    for task_id, job in Taskman.jobs.items():
        if job.status == JobStatus.Finished and _match(task_name, job.name):
            filepath = job.name + '/' + job.task_id + '/results.csv'
            files.append(filepath)
    # Call pack.sh
    subprocess.Popen([HOMEDIR + '/taskman/packresults.sh'] + files)


def clean(task_name=None):
    shutil.copyfile(DB_STARTED_TASKS,
                    HOMEDIR + '/taskman/old/started_' + datetime.now().strftime("%m-%d_%H-%M-%S"))

    started_tasks, dead_tasks, finished_tasks = Taskman.read_task_db()

    with open(DB_STARTED_TASKS, 'w') as f:
        for task_id, fields in started_tasks.items():
            name, moab_id, template_file, args_str = fields
            remove = moab_id in dead_tasks or moab_id in finished_tasks
            if task_name is not None:
                remove = _match(task_name, name) and remove
            if not remove:
                job = Job(task_id, name, moab_id, None, template_file, args_str)
                Taskman.write_started(job, f)


# Available commands
cmds = {'sub': submit, 'fromckpt': fromckpt, 'multisub': multi_sub, 'cont': continu, 'cancel': cancel, 'copy': copy,
        'pack': pack, 'results': results, 'show': show, 'clean': clean}
