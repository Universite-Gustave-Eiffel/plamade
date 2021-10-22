#! /bin/bash
#SBATCH -a 0-23   # execute tasks from 0 to 23

# run with this command
# sbatch noisemodelling_batch.sh
dbpath="/home2020/home/cerema/nfortin/dep38/"
codepath="/home2020/home/cerema/nfortin/plamade/plamade/computation_core"

echo "copy data to local node"
mkdir /scratch/job."$SLURM_JOB_ID"/data/
rsync -a "$dbpath" /scratch/job."$SLURM_JOB_ID"/data/

echo "copy code to local node"
mkdir /scratch/job."$SLURM_JOB_ID"/code/
rsync -a "$codepath"/build /scratch/job."$SLURM_JOB_ID"/code/

echo "run noisemodelling"
cd /scratch/job."$SLURM_JOB_ID"/code && java -jar build/libs/computation_core.jar -n"$SLURM_ARRAY_TASK_ID" -w/scratch/job."$SLURM_JOB_ID"/data/

echo "copy results"
mkdir -p ~/job_"$SLURM_ARRAY_JOB_ID"
cp /scratch/job."$SLURM_JOB_ID"/data/*.shp ~/job_"$SLURM_ARRAY_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.dbf ~/job_"$SLURM_ARRAY_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.shx ~/job_"$SLURM_ARRAY_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.prj ~/job_"$SLURM_ARRAY_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.csv ~/job_"$SLURM_ARRAY_JOB_ID"/
