#!/bin/sh -ex

. /opt/etcd/etcd-lib.sh
export ETCDCTL_ENDPOINTS="$(seed_endpoints)"

# Remove this member from the cluster peer set.
if [ $(member_index) -ge ${MIN_REPLICAS} ]; then
  echo "Removing ${HOSTNAME} from etcd cluster."
  if etcdctl ${AUTH_FLAGS} member remove $(member_hash); then
    # Remove everything, otherwise the cluster will no longer scale back
    # up again if this volume is reused.
    rm -rf /var/run/etcd/*
  fi
fi
