from os import environ as env_vars
from os.path import expandvars


HOMEDIR = expandvars('$HOME')
DB_STARTED_TASKS = HOMEDIR + '/taskman/started'
SCRIPTS_FOLDER = env_vars.get('TASKMAN_SCRIPTS', HOMEDIR + '/script_moab')  # Dir with your scripts. Contains /taskman
CKPT_FOLDER = env_vars['TASKMAN_CKPTS']
SLURM_MODE = 'TASKMAN_USE_SLURM' in env_vars

from .taskman import Taskman
