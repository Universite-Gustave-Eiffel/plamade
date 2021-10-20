#! /bin/bash
# run with this command
# srun -n 1 --tasks-per-node=1 sh plamade.sh
dbpath="/home2020/home/cerema/nfortin/dep38/"

# copy gradle cache (faster build time)
echo "Sync gradle cache"
rsync -a --stats ~/.gradle/ /scratch/job."$SLURM_JOB_ID"/.gradle/
export GRADLE_USER_HOME=/scratch/job."$SLURM_JOB_ID"/.gradle/

echo "gradle path is now $GRADLE_USER_HOME"

echo "copy data to local node"
mkdir /scratch/job."$SLURM_JOB_ID"/data/
rsync -a "$dbpath" /scratch/job."$SLURM_JOB_ID"/data/

echo "copy code to local node"
mkdir /scratch/job."$SLURM_JOB_ID"/code/
rsync -a ../ /scratch/job."$SLURM_JOB_ID"/code

echo "run noisemodelling"
cd /scratch/job."$SLURM_JOB_ID"/code && ./gradlew computation_core:run --args="-n$SLURM_NODEID -w/scratch/job.$SLURM_JOB_ID/data/"
echo "copy results"
mkdir -p ~/job_"$SLURM_JOB_ID"
cp /scratch/job."$SLURM_JOB_ID"/data/*.shp ~/job_"$SLURM_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.dbf ~/job_"$SLURM_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.shx ~/job_"$SLURM_JOB_ID"/
cp /scratch/job."$SLURM_JOB_ID"/data/*.prj ~/job_"$SLURM_JOB_ID"/
