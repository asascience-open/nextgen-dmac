# Argo Workflows Test of QARTOD and IOOS Compliance Checker

An example workflow for running IOOS Compliance Checker for Climate and Forecast (CF) compliance checking along with QARTOD quality control tests run against a NetCDF file.

## Running

Ensure that the ConfigMap provided in `artifact_repo.yaml` is set up, along with secrets added for AWS S3 IAM access and secret keys using Kubernetes secrets.  Ensure Argo CLI is installed and functional.

Install all required dependencies with `conda install --file requirements_conda.txt`

Use `jupyter notebook initialize_datasets.ipynb` and load an ERDDAP dataset, configure QC tests as desired, and then finally submit the job to the Argo cluster.

Outputs in S3 are created for Compliance Checker in `cchecker_report.json` and for IOOS QC/QARTOD, `cchecker_report.json`.

## TODO (possibly move to GitHub issues):

- Install `cf-units` and create reasonable bounds for QC.
- Consider use of `panel`instead of `ipywidgets`.
- Additional tests, such as location test with bounding box map widget for bounds selection.
- Set up dynamic filenames based upon input filenames from `sprig` templating functions.
- Suppress error codes from routine compliance checker errors.
- Signal RabbitMQ when workflows finish.
