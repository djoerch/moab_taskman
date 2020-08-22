#!/usr/bin/env bash

# kwargs:
#  --path-to-repo - path to taskman repository (to copy template files from here)
#  --path-to-taskman-root - path to taskman root folder to be setup
#
# note:
#  Script must be sourced to make setting environment variables take effect.
#
# example call:
#  . ${HOME}/git/repositories/moab_taskman/templates/setup_taskman.sh \
#    --path-to-repo ${HOME}/git/repositories/moab_taskman \
#    --path-to-taskman-root ${HOME}/taskman


POSITIONAL=()
while [[ $# -gt 0 ]]
do
    key="$1"

    case ${key} in
        --path-to-repo)
            PATH_TO_REPO="$2"
            shift # past argument
            shift # past value
            ;;
        --path-to-taskman-root)
            PATH_TO_TASKMAN_ROOT="$2"
            shift # past argument
            shift # past value
            ;;
        *)    # unknown option
            POSITIONAL+=("$1") # save it in an array for later
            shift # past argument
            ;;
    esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

COMMAND="${@}"

echo
echo " ** ** Setting up taskman environment ** ** "
echo " - - - - - - - "
echo " Taskman repo  : ${PATH_TO_REPO}"
echo " Taskman root  : ${PATH_TO_TASKMAN_ROOT}"
echo " - - - - - - - "
echo

# checks
if [[ -z "${PATH_TO_REPO}" || -z "${PATH_TO_TASKMAN_ROOT}" ]]
then

    echo "Not all required information provided!"
    exit -1

fi

if [[ $# -gt 0 ]]
then

    echo "Too many arguments!"
    exit -1

fi


# create root folder
mkdir -p ${PATH_TO_TASKMAN_ROOT}/scripts
mkdir -p ${HOME}/logs

# copy template scripts
cp ${PATH_TO_REPO}/templates/template.sh ${PATH_TO_TASKMAN_ROOT}/scripts/
cp ${PATH_TO_REPO}/templates/taskman_post_exec.sh ${PATH_TO_TASKMAN_ROOT}/scripts/

# setup environment variables
export TASKMAN_USE_SLURM=True
export TASKMAN_CKPTS=${PATH_TO_TASKMAN_ROOT}/checkpoints
export TASKMAN_SCRIPTS=${PATH_TO_TASKMAN_ROOT}/scripts
