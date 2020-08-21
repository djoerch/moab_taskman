#!/usr/bin/env python

import time

from taskman import Taskman
from taskman.commands import handle_command, show_commands
from taskman.utils import assert_environment_exists


if __name__ == '__main__':

    assert_environment_exists()

    while True:

        command_mode = False

        try:

            Taskman.update()
            time.sleep(10)

        except KeyboardInterrupt:
            command_mode = True

        if command_mode:

            print('\rUpdating, please wait...')

            Taskman.update(resume_incomplete_tasks=False)
            show_commands()
            command = input('\033[1mCommand>>\033[0m ')
            handle_command(command)
