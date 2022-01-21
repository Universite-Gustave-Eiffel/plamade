#! /bin/bash
#SBATCH -a 0-23   # execute tasks from 0 to 23

# run with this command
# must be run in the same folder than the database
# sbatch noisemodelling_batch.sh

echo "copy data and code to local node"
rsync -a ./ /scratch/job."$SLURM_JOB_ID"/

echo "run noisemodelling"
cd /scratch/job."$SLURM_JOB_ID"/ && java -jar build/libs/computation_core.jar -n"$SLURM_ARRAY_TASK_ID" -w/scratch/job."$SLURM_JOB_ID"/

echo "copy results"
mkdir -p ~/results
cp /scratch/job."$SLURM_JOB_ID"/data/*.shp ./results/
cp /scratch/job."$SLURM_JOB_ID"/data/*.dbf ./results/
cp /scratch/job."$SLURM_JOB_ID"/data/*.shx ./results/
cp /scratch/job."$SLURM_JOB_ID"/data/*.prj ./results/
cp /scratch/job."$SLURM_JOB_ID"/data/*.csv ./results/
