# Kerchunk ingest

This folder houses a generic debian docker image that can run arbitrary kerchunk scripts. The main use case is for as a use in [argo-workflows](https://argoproj.github.io/argo-workflows), where the image can be used to compose kerchunk workflows as results of [events](https://argoproj.github.io/argo-events/).

Aside from the dockerfile, there are also workflows and kerchunk python scripts to ingest IOOS model data files as kerchunked files.

### Notes

Test `argo-events` using the webhooks source example: https://argoproj.github.io/argo-events/quick_start/

Clear `Success` or `Error` Pods: 

```bash
kubectl delete pod --field-selector=status.phase==Succeeded -n argo-events
kubectl delete pod --field-selector=status.phase==Failed -n argo-events
```