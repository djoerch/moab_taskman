import json
import subprocess
import inspect
import time
from datetime import datetime
from enum import Enum
from os import makedirs
from os.path import expandvars

homedir = expandvars('$HOME')


def fmt_time(seconds):
    if seconds < 60:
        return str(round(seconds)) + 's'
    else:
        return str(round(seconds / 60)) + 'm'


class JobStatus(Enum):
    Dead = 'Dead'
    Finished = 'Finished'
    Unknown = '?'
    Running = 'Running'
    Waiting = 'Waiting'
    Blocked = 'Blocked'
    Lost = 'Lost'

    def __str__(self):
        return self.value

    @property
    def cancellable(self):
        return self in [JobStatus.Running, JobStatus.Waiting, JobStatus.Blocked]

    @property
    def needs_attention(self):
        return self in [JobStatus.Dead, JobStatus.Lost]


class Job(object):
    def __init__(self, task_id, name, moab_id, status, template_file, args_str):
        self.task_id = task_id
        self.moab_id = moab_id
        self.name = name
        self.status = status
        self.template_file = template_file
        self.args_str = args_str
        self.report = {}

    @property
    def script_file(self):
        _, script_file = Job.get_path(self.name, self.task_id)
        return script_file

    @staticmethod
    def get_path(task_name, task_id):
        script_path = homedir + '/script_moab/taskman/' + task_name
        script_file = script_path + '/' + task_id + '.sh'
        return script_path, script_file


class Taskman(object):
    jobs = {}
    columns = set()

    @staticmethod
    def get_cmd_output(args, timeout=20):
        try:
            output = subprocess.check_output(args, stderr=subprocess.STDOUT, timeout=timeout)
        except subprocess.CalledProcessError as e:
            print('Error with command: ' + ' '.join(args))
            print(e.output)
            raise
        except subprocess.TimeoutExpired as e:
            print('Timeout with command: ' + ' '.join(args))
            print(e.output)
            return None
        return output.decode('UTF-8')

    @staticmethod
    def get_moab_queue():
        args = ['showq', '-w', expandvars('user=$USER'), '--blocking']
        output = Taskman.get_cmd_output(args, timeout=10)
        if output is None:
            return None, None, None

        showq_lines = output.split('\n')
        showq_lines = [l.strip() for l in showq_lines]
        lists = {'active j': [], 'eligible': [], 'blocked ': []}
        cur_list = None
        for line in showq_lines:
            if line[:8] in lists:
                cur_list = line[:8]
            elif line != '' and \
                            'JOBID' not in line and \
                            'processors' not in line and \
                            'nodes' not in line and \
                            'eligible' not in line and \
                            'Total' not in line and \
                            'blocked' not in line:
                moab_id = line.split(' ')[0]
                lists[cur_list].append(moab_id)
        return lists['active j'], lists['eligible'], lists['blocked ']

    @staticmethod
    def create_task(template_file, args_str, task_name):
        # Generate id
        task_id = datetime.now().strftime("%m-%d_%H-%M-%S_%f")
        script_path, script_file = Job.get_path(task_name, task_id)

        # Get template
        with open(homedir + '/script_moab/' + template_file + '.sh', 'r') as f:
            template = f.readlines()

        # Append post exec bash script
        with open(homedir + '/script_moab/taskman_post_exec.sh', 'r') as f:
            post_exec = f.readlines()
        template += post_exec

        # Replace variables
        script_lines = []
        for line in template:
            line = line.replace('$TASKMAN_NAME', task_name)
            line = line.replace('$TASKMAN_ID', task_id)
            line = line.replace('$TASKMAN_ARGS', args_str)
            script_lines.append(line)

        # Write script
        makedirs(script_path, exist_ok=True)
        with open(script_file, 'w') as f:
            f.writelines(script_lines)

        print('Created', script_file)
        return Job(task_id, task_name, None, None, template_file, args_str)

    @staticmethod
    def submit(job):
        # Submit using msub
        print('Calling msub...', end=' ')
        output = Taskman.get_cmd_output(['msub', job.script_file])
        if output is None:
            return

        # Get moab job id
        moab_id = output.strip()

        # Add to 'started' database
        with open(homedir + '/taskman/started', 'a') as f:
            line = '{};{};{};{};{}'.format(job.task_id, job.name, moab_id, job.template_file, job.args_str)
            f.write(line + '\n')

        print('Submitted.  TaskmanID: {}  MoabID: {}'.format(job.task_id, moab_id))

    @staticmethod
    def cancel(task_id):
        job = Taskman.jobs[task_id]
        output = Taskman.get_cmd_output(['mjobctl', '-c', job.moab_id])
        if output is None:
            return

        # Add to 'finished' database
        with open(homedir + '/taskman/finished', 'a') as f:
            line = '{},{},{}'.format(job.moab_id, job.name, 'cancel')
            f.write(line + '\n')

        print(output.strip())

    @staticmethod
    def update_job_list():
        active_jobs, eligible_jobs, blocked_jobs = Taskman.get_moab_queue()

        with open(homedir + '/taskman/started', 'r') as f:
            started_tasks_csv = f.readlines()
        with open(homedir + '/taskman/dead', 'r') as f:
            dead_tasks_csv = f.readlines()
        with open(homedir + '/taskman/finished', 'r') as f:
            finished_tasks_csv = f.readlines()

        started_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(';') for l in started_tasks_csv]}
        dead_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in dead_tasks_csv]}
        finished_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in finished_tasks_csv]}

        jobs = {}
        for task_id, fields in sorted(started_tasks.items(), key=lambda x: x[1][0]):
            name, moab_id, template_file, args_str = fields
            if moab_id in dead_tasks:
                status = JobStatus.Dead
            elif moab_id in finished_tasks:
                status = JobStatus.Finished
            elif active_jobs is None:
                status = JobStatus.Unknown  # showq has timed out
            elif moab_id in active_jobs:
                status = JobStatus.Running
            elif moab_id in eligible_jobs:
                status = JobStatus.Waiting
            elif moab_id in blocked_jobs:
                status = JobStatus.Blocked
            else:
                status = JobStatus.Lost

            jobs[task_id] = Job(task_id, name, moab_id, status, template_file, args_str)
        Taskman.jobs = jobs
        Taskman.update_report()

    @staticmethod
    def update_report():
        Taskman.columns = set()
        for task_id, job in Taskman.jobs.items():
            output_filepath = homedir + '/logs/' + job.name + '.o' + job.moab_id
            report_line = None
            try:
                with open(output_filepath, 'r') as f:
                    for line in f:
                        if line[:8] == '!taskman':
                            report_line = line
            except FileNotFoundError:
                pass
            if report_line is not None:
                job.report = json.loads(report_line[8:])
                Taskman.columns.update(job.report.keys())
        if 'time' in Taskman.columns:
            Taskman.columns.remove('time')

    @staticmethod
    def show_status():
        print('\033[2J\033[H')  # Clear screen and move cursor to top left
        print('\033[97;45m( Moab Task Manager )\033[0m     ' + time.strftime("%H:%M:%S"), end='')
        print('     \033[37mCtrl+C to enter command mode\033[0m')

        line_fmt = '{:<8} {:<30} {:<19} {:<7} {:<7}' + ' {:<12}' * len(Taskman.columns)
        print('\033[1m' + line_fmt.format('Status', 'Task name', 'Task id', 'Moab id', 'Updated',
                                          *sorted(Taskman.columns)) + '\033[0m')
        for task_id, job in sorted(Taskman.jobs.items(), key=lambda x: x[1].name):
            # Get report data
            report_columns = []
            for k in sorted(Taskman.columns):
                val_str = str(job.report.get(k, ''))[:12]
                report_columns.append(val_str)
            time_ago = fmt_time(time.time() - job.report['time']) if 'time' in job.report else ''
            # Format line
            status_line = line_fmt.format(job.status, job.name, task_id, job.moab_id, time_ago, *report_columns)
            if job.status.needs_attention:
                status_line = '\033[31m' + status_line + '\033[0m'
            print(status_line)


def _handle_command(cmd_str):
    tokens = cmd_str.split(' ')
    cmd_name = tokens[0]
    if cmd_name == '':
        return
    cmd_args = ' '.join(tokens[1:])
    cmds[cmd_name](*cmd_args.split(';'))


def _show_commands():
    print('-------------------')
    print('Available commands:')
    for name, fn in cmds.items():
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        print(name, ':', '; '.join([str(p) for p in params]))


def _match(pattern, name):
    match = lambda x: x.startswith(pattern[:-1]) if pattern.endswith('*') else lambda x: x == pattern
    return match(name)


def submit(template_file, args_str, task_name):
    job = Taskman.create_task(template_file, args_str, task_name)
    Taskman.submit(job)


def continu(task_name):
    for task_id, job in Taskman.jobs.items():
        if job.status == JobStatus.Finished and _match(task_name, job.name):
            Taskman.submit(job)


def cancel(task_name):
    for task_id, job in Taskman.jobs.items():
        if job.status.cancellable and _match(task_name, job.name):
            Taskman.cancel(task_id)


def copy(task_name):
    for task_id, job in Taskman.jobs.items():
        if _match(task_name, job.name):
            job = Taskman.create_task(job.template_file, job.args_str, job.name)
            Taskman.submit(job)


# Available commands
cmds = {'sub': submit, 'cont': continu, 'cancel': cancel, 'copy': copy}


if __name__ == '__main__':
    taskman = Taskman()
    while True:
        command_mode = False
        try:
            taskman.update_job_list()
            taskman.show_status()
            time.sleep(4)
        except KeyboardInterrupt:
            command_mode = True

        if command_mode:
            print()
            _show_commands()
            command = input('\033[1mCommand>>\033[0m ')
            _handle_command(command)
