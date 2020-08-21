import subprocess
import json
import time
from datetime import datetime
from os import makedirs
from os.path import expandvars

from . import SLURM_MODE, SCRIPTS_FOLDER, DB_STARTED_TASKS, HOMEDIR
from .job import Job, JobStatus
from .utils import fmt_time


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
    def get_queue():
        if SLURM_MODE:
            return Taskman.get_slurm_queue()
        else:
            return Taskman.get_moab_queue()

    @staticmethod
    def get_moab_queue():
        args = ['showq', '-w', expandvars('user=$USER'), '--blocking']
        output = Taskman.get_cmd_output(args, timeout=10)
        if output is None:
            return None

        showq_lines = output.split('\n')
        showq_lines = [l.strip() for l in showq_lines]
        lists = {'active j': [], 'eligible': [], 'blocked ': []}
        cur_list = None
        statuses = {}
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
                statuses[moab_id] = cur_list
        return statuses

    @staticmethod
    def get_slurm_queue():
        args = ['squeue', '-u', expandvars('$USER')]
        output = Taskman.get_cmd_output(args, timeout=10)
        if output is None:
            return None

        showq_lines = output.split('\n')
        showq_lines = [l for l in showq_lines]
        statuses = {}
        for line in showq_lines[1:]:  # skip header
            slurm_id = line[:8].strip()
            slurm_state = line[47:50].strip()
            statuses[slurm_id] = slurm_state
        return statuses

    @staticmethod
    def create_task(template_file, args_str, task_name):
        # Generate id
        task_id = datetime.now().strftime("%m-%d_%H-%M-%S_%f")
        script_path, script_file = Job.get_path(task_name, task_id)

        # Get template
        with open(SCRIPTS_FOLDER + '/' + template_file + '.sh', 'r') as f:
            template = f.readlines()

        # Append post exec bash script
        with open(SCRIPTS_FOLDER + '/taskman_post_exec.sh', 'r') as f:
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
    def write_started(job, db_file=None):
        if db_file is None:
            f = open(DB_STARTED_TASKS, 'a')
        else:
            f = db_file

        line = '{};{};{};{};{}'.format(job.task_id, job.name, job.moab_id, job.template_file, job.args_str)
        f.write(line + '\n')

        if db_file is None:
            f.close()

    @staticmethod
    def submit(job):
        subm_command = 'sbatch' if SLURM_MODE else 'msub'

        print('Calling ' + subm_command + '...', end=' ')
        output = Taskman.get_cmd_output([subm_command, job.script_file])
        if output is None:
            return

        job.moab_id = output.strip().split(' ')[-1]

        # Add to 'started' database
        Taskman.write_started(job)

        print('Submitted.  TaskmanID: {}  Moab/SLURM ID: {}'.format(job.task_id, job.moab_id))

    @staticmethod
    def cancel(task_id):
        job = Taskman.jobs[task_id]
        cmd_tokens = ['scancel', job.moab_id] if SLURM_MODE else ['mjobctl', '-c', job.moab_id]

        output = Taskman.get_cmd_output(cmd_tokens)
        if output is None:
            return

        # Add to 'finished' database
        with open(HOMEDIR + '/taskman/finished', 'a') as f:
            line = '{},{},{}'.format(job.moab_id, job.name, 'cancel')
            f.write(line + '\n')

        print(output.strip())

    @staticmethod
    def read_task_db():
        with open(HOMEDIR + '/taskman/started', 'r') as f:
            started_tasks_csv = f.readlines()
        with open(HOMEDIR + '/taskman/dead', 'r') as f:
            dead_tasks_csv = f.readlines()
        with open(HOMEDIR + '/taskman/finished', 'r') as f:
            finished_tasks_csv = f.readlines()

        if started_tasks_csv[0].strip() == '':
            started_tasks = None
        else:
            started_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(';')
                                                                  for l in started_tasks_csv if l.strip() != '']}
        dead_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in dead_tasks_csv]}
        finished_tasks = {tokens[0]: tokens[1:] for tokens in [l.strip().split(',') for l in finished_tasks_csv]}
        return started_tasks, dead_tasks, finished_tasks

    @staticmethod
    def update_job_list():
        statuses = Taskman.get_queue()

        started_tasks, dead_tasks, finished_tasks = Taskman.read_task_db()
        if started_tasks is None:
            return

        jobs = {}

        for task_id, fields in sorted(started_tasks.items(), key=lambda x: x[1][0]):
            name, moab_id, template_file, args_str = fields
            j = Job(task_id, name, moab_id, None, template_file, args_str)

            if moab_id in dead_tasks:
                j.status = JobStatus.Dead
            elif moab_id in finished_tasks:
                j.status = JobStatus.Finished
                j.finish_msg = finished_tasks[moab_id][1]
            else:
                if statuses is None:
                    j.status = JobStatus.Unknown  # showq has timed out
                elif moab_id not in statuses:
                    j.status = JobStatus.Lost
                elif statuses[moab_id] in ['R', 'active j']:
                    j.status = JobStatus.Running
                elif statuses[moab_id] in ['PD', 'eligible']:
                    j.status = JobStatus.Waiting
                else:
                    j.status = JobStatus.Other
                    j.status_msg = statuses[moab_id]

            jobs[task_id] = j
        Taskman.jobs = jobs
        Taskman.update_report()

    @staticmethod
    def get_log(job, error_log=False):
        ext_prefix = '.e' if error_log else '.o'
        output_filepath = HOMEDIR + '/logs/' + job.name + ext_prefix + job.moab_id
        with open(output_filepath, 'r') as f:
            lines = f.readlines()
        return lines

    @staticmethod
    def update_report():
        Taskman.columns = set()
        for task_id, job in Taskman.jobs.items():
            report_line = None
            try:
                log_lines = Taskman.get_log(job)
                for line in log_lines:
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
    def resume_incomplete_tasks():
        for task_id, job in Taskman.jobs.items():
            if job.status != JobStatus.Finished:
                continue
            do_resubmit = job.report.get('resubmit', False)
            if do_resubmit:
                Taskman.submit(job)
        time.sleep(2)

    @staticmethod
    def show_status():
        print('\033[2J\033[H')  # Clear screen and move cursor to top left
        print('\033[97;45m( Experiment Manager )\033[0m     ' + time.strftime("%H:%M:%S"), end='')
        print('     \033[37mCtrl+C to enter command mode\033[0m')

        line_fmt = '{:<8} {:<30} {:<21} {:<7} {:<7}' + ' {:<12}' * len(Taskman.columns)
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
            elif job.status == JobStatus.Other:
                status_line = '\033[30;47m' + job.status_msg[:8].ljust(8) + status_line[8:] + '\033[0m'
            elif job.status == JobStatus.Finished:
                finished_status = {'ok': '\033[32;107mFinished\033[;107m',  # Green
                                   'cancel': '\033[;107mCancel\'d'  # Black
                                   }.get(job.finish_msg, '\033[;107mFinished')
                status_line = finished_status + status_line[8:] + '\033[0m'
            print(status_line)

    @staticmethod
    def update(resume_incomplete_tasks=True):
        Taskman.update_job_list()
        Taskman.show_status()
        if resume_incomplete_tasks:
            Taskman.resume_incomplete_tasks()
