from enum import Enum

from . import SCRIPTS_FOLDER


class JobStatus(Enum):
    Dead = 'Dead'
    Finished = 'Finished'
    Unknown = '?'
    Running = 'Running'
    Waiting = 'Waiting'
    Lost = 'Lost'
    Other = ''

    def __str__(self):
        return self.value

    @property
    def cancellable(self):
        return self in [JobStatus.Running, JobStatus.Waiting]

    @property
    def needs_attention(self):
        return self in [JobStatus.Dead, JobStatus.Lost]


class Job(object):
    def __init__(self, task_id, name, moab_id, status, template_file, args_str):
        self.task_id = task_id
        self.moab_id = moab_id
        self.name = name
        self.status = status
        self.status_msg = None
        self.template_file = template_file
        self.args_str = args_str
        self.report = {}
        self.finish_msg = ''

    @property
    def script_file(self):
        _, script_file = Job.get_path(self.name, self.task_id)
        return script_file

    @staticmethod
    def get_path(task_name, task_id):
        script_path = SCRIPTS_FOLDER + '/taskman/' + task_name
        script_file = script_path + '/' + task_id + '.sh'
        return script_path, script_file
