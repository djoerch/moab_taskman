#!/bin/bash

#SBATCH --time=10:00:00
#SBATCH --account=def-descotea
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4
#SBATCH --output=/home/jord2201/logs/$TASKMAN_NAME.o%j
#SBATCH --error=/home/jord2201/logs/$TASKMAN_NAME.e%j


module load singularity

# Set shutdown time
export HANGUP_TIME=$(($(date +"%s") + 10 * 3600))

cd ${HOME}/git/repositories/vslic/containerization/singularity

bash vslic_run_in_singularity.sh \
  --data-mount-path ${SCRATCH}/vslic_test_dir \
  --venv-mount-path ${SCRATCH}/vslic_test_venv_3 \
  --code-mount-path ${HOME}/git/repositories/vslic \
  --singularity-image ${SCRATCH}/vslic-3.sif \
  vslic_prepare_data.py -c ${SCRATCH}/vslic_test_dir/configs/$TASKMAN_ARGS
