#! /bin/bash
#SBATCH --ntasks=4
#SBATCH --tasks-per-node=1
#SBATCH -t 4:00:00     # Le job sera tué au bout de 4h
#SBATCH --mem=4096      # mem per node in megabytes

SECONDS=0

srun sh slurm.sh

echo "Computed in $SECONDS seconds" > ~/job_"$SLURM_JOB_ID"/ctime.txt



