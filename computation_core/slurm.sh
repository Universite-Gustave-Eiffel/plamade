#! /bin/bash
# run with this command
# srun -n 1 --tasks-per-node=1 sh plamade.sh
dbpath="/home2020/home/cerema/nfortin/dep38"


# copy database to local node
cp "$dbpath"/h2gisdb.mv.db /scratch/job."$SLURM_JOB_ID"/h2gisdb.mv.db
# copy node configuration
cp "$dbpath"/cluster_config.json /scratch/job."$SLURM_JOB_ID"/cluster_config.json
cd ../ && ./gradlew computation_core:run --offline -x --args="-n$SLURM_NODEID -w/scratch/job.$SLURM_JOB_ID/"
cp /scratch/job."$SLURM_JOB_ID"/h2gisdb.mv.db "$dbpath"/h2gisdb_"$SLURM_JOB_ID"_"$SLURM_NODEID".mv.db
