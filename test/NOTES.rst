Notes on setting up and testing via Minikube
============================================


Start minikube::

  sudo minikube start --vm-driver=none --extra-config=apiserver.authorization-mode=RBAC

Install Tiller::

  kubectl create serviceaccount -n kube-system tiller
  kubectl create clusterrolebinding tiller-binding --clusterrole=cluster-admin --serviceaccount kube-system:tiller
  kubectl create clusterrolebinding fixDNS --clusterrole=cluster-admin --serviceaccount=kube-system:kube-dns

  helm repo update
  helm init --service-account tiller

Bootstrap an Etcd cluster using `Etcd operator <https://coreos.com/blog/introducing-the-etcd-operator.html)>`_ ::

  helm install stable/etcd-operator --name etcd-operator
  kubectl create -f test/etcd-cluster.yaml

Start ``minio`` to provide an S3-compatible cloud filesystem. Use the following values::

  helm install stable/minio --name minio -f test/minio-values.yaml

This will setup the ``examples`` bucket. If needed, additional buckets can be created
by port-forwarding to the minio service, and using the web UI and the configured
access & secret key (typically the defaults of ``AKIAIOSFODNN7EXAMPLE`` and
``wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY``)::

  alias minio_pod="kubectl get pod -l release=minio -o jsonpath={.items[0].metadata.name}"
  kubectl port-forward $(minio_pod) 9000:9000
  browse http://localhost:9000

Start a Gazette cluster::

  helm install charts/gazette --name gazette

Start the ``stream-sum`` example, which also doubles as an end-to-end integration test::

  helm install charts/examples/stream-sum --name stream-sum

Start the ``word-count`` application::

  helm install charts/examples/word-count --name word-count

Port-forward a Gazette pod endpoint to localhost::

  alias gazette_pod="kubectl get pod -l app=gazette -o jsonpath={.items[0].metadata.name}"
  kubectl port-forward $(gazette_pod) 8081:8081

Create the example journals::

  gazctl journal apply -s test/journalspace.yaml

Begin a long-lived read of output counts::

  curl -L -v "http://localhost:8081/examples/word-count/counts?block=true&offset=-1"

Load a bunch of data into the input sentences journal::

  curl -v -X PUT http://localhost:8081/examples/word-count/sentences --data-binary @a_tale_of_two_citites.txt

