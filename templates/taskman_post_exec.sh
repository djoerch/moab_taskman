
# Post_exec: check if job was actually finished
exec_status=${?}

if [[ ${exec_status} -eq 0 ]]
then

    # Finished without errors
    echo "$MOAB_JOBID,$TASKMAN_NAME,ok" >> ${HOME}/taskman/finished

else

    echo "$MOAB_JOBID,$TASKMAN_NAME,${exec_status}" >> ${HOME}/taskman/dead

fi
