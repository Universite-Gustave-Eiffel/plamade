#!/bin/bash

# 1. Create the job-specific directory
# Use a dot or underscore; job.${SLURM_JOB_ID} is fine
mkdir -p /scratch/job.${SLURM_JOB_ID}

# 2. Change ownership to the user who submitted the job
# $SLURM_JOB_USER is the username
# $SLURM_JOB_UID is the numeric ID (even more reliable)
if [ -n "$SLURM_JOB_USER" ]; then
    chown "$SLURM_JOB_USER" /scratch/job.${SLURM_JOB_ID}
    chmod 700 /scratch/job.${SLURM_JOB_ID}
fi
