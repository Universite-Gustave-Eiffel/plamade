#! /bin/bash
#SBATCH -a 0-11   # execute tasks from 0 to 23

# run with this command
# must be run in the same folder than the database
# sbatch noisemodelling_batch.sh

echo "copy data and code to local node"
rsync -a ./ /scratch/job."$SLURM_JOB_ID"/data/

echo "run noisemodelling"
cd /scratch/job."$SLURM_JOB_ID"/data/ && java -jar build/libs/computation_core.jar -n"$SLURM_ARRAY_TASK_ID" -w/scratch/job."$SLURM_JOB_ID"/data/

echo "copy results"
mkdir -p ~/results_"$SLURM_ARRAY_JOB_ID"
cp /scratch/job."$SLURM_JOB_ID"/data/*.shp ~/results_"$SLURM_ARRAY_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.dbf ~/results_"$SLURM_ARRAY_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.shx ~/results_"$SLURM_ARRAY_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.prj ~/results_"$SLURM_ARRAY_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.csv ~/results_"$SLURM_ARRAY_JOB_ID"/
