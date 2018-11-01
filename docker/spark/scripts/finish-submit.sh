#!/bin/bash

# SUPPORT FOR NOMAD
if [[ "${KILL_NOMAD_JOB_ON_COMPLETE}" == true ]] ; then
    cmd="curl --header \"X-Nomad-Token: ${NOMAD_SECRET_KEY}\"  --request DELETE ${NOMAD_URL}/v1/job/${NOMAD_JOB_NAME}"
    echo "Running command: ${cmd}"
    eval ${cmd}
    rc=$?
    if [[ ${rc} != 0 ]]; then
        echo "Failed to stop nomad job"
        exit ${rc}
    fi
fi
echo -e "\nJob completed\n"

