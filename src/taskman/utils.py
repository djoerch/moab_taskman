import os

from pathlib import Path

from taskman import HOMEDIR, CKPT_FOLDER, SCRIPTS_FOLDER


def fmt_time(seconds):
    if seconds >= 3600:
        return str(round(seconds / 3600)) + 'h'
    elif seconds >= 60:
        return str(round(seconds / 60)) + 'm'
    else:
        return str(round(seconds)) + 's'


def assert_environment_exists():

    # assert folders exist
    folders = [os.path.join(HOMEDIR, 'taskman'), CKPT_FOLDER, SCRIPTS_FOLDER]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)

    # assert dbs exist
    databases = ['started', 'dead', 'finished']
    for db in databases:
        Path(os.path.join(HOMEDIR, 'taskman', db)).touch()
