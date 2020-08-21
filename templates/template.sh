#!/bin/bash

#SBATCH --time=00:15:00
#SBATCH --account=def-descotea
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4

module load singularity

cd ${HOME}/git/repositories/vslic/containerization/singularity

bash vslic_run_in_singularity.sh \
  --data-mount-path ${SCRATCH}/vslic_test_dir \
  --venv-mount-path ${SCRATCH}/vslic_test_venv \
  --code-mount-path ${HOME}/git/repositories/vslic \
  --singularity-image ${SCRATCH}/vslic-3.sif \
  vslic_classify_streamlines.py -c ${SCRATCH}/vslic_test_dir/configs/$TASKMAN_ARGS
