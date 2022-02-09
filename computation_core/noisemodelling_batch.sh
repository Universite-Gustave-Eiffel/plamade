#! /bin/bash

# run with this command
# must be run in the same folder than the database
# sbatch --array=0-11 noisemodelling_batch.sh

echo "copy data and code to local node"
rsync -a ./ /scratch/job."$SLURM_JOB_ID"/data/

echo "run noisemodelling"
cd /scratch/job."$SLURM_JOB_ID"/data/ && /home2020/home/cerema/nfortin/jdk-11.0.13/bin/java -jar libs/computation_core.jar -n"$SLURM_ARRAY_TASK_ID" -w/scratch/job."$SLURM_JOB_ID"/data/

echo "copy results"
mkdir -p ~/results_"$SLURM_ARRAY_JOB_ID"

# copy files, ignore missing documents
cp /scratch/job."$SLURM_JOB_ID"/data/*.shp ~/results_"$SLURM_ARRAY_JOB_ID"/ 2>/dev/null || :
cp /scratch/job."$SLURM_JOB_ID"/data/*.dbf ~/results_"$SLURM_ARRAY_JOB_ID"/ 2>/dev/null || :
cp /scratch/job."$SLURM_JOB_ID"/data/*.shx ~/results_"$SLURM_ARRAY_JOB_ID"/ 2>/dev/null || :
cp /scratch/job."$SLURM_JOB_ID"/data/*.prj ~/results_"$SLURM_ARRAY_JOB_ID"/ 2>/dev/null || :
cp /scratch/job."$SLURM_JOB_ID"/data/*.csv ~/results_"$SLURM_ARRAY_JOB_ID"/ 2>/dev/null || :
cp /scratch/job."$SLURM_JOB_ID"/data/*.kml ~/results_"$SLURM_ARRAY_JOB_ID"/ 2>/dev/null || :
cp /scratch/job."$SLURM_JOB_ID"/data/*.sql ~/results_"$SLURM_ARRAY_JOB_ID"/ 2>/dev/null || :
