from dask_gateway import Gateway
gateway = Gateway(
    "http://k8s-daskgate-traefikd-b0c7f98d0b-9404246f96ed5ac3.elb.us-east-1.amazonaws.com",
)
gateway.list_clusters()