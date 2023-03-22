import ioos_qc
import sys
import argparse
import xarray
import json

from ioos_qc.config import QcConfig
from ioos_qc.streams import XarrayStream
# can't seem to make CFNetCDFStore work with TimeseriesProfile CDM
#from ioos_qc.stores import CFNetCDFStore
import argparse
from ioos_qc.stores import PandasStore
from ioos_qc.results import collect_results, CollectedResult

#import pocean.dsg
import sys

def main(config_str: str, x_var_str: str, y_var_str: str, z_var_str: str,
         t_var_str: str):

    config_dict = json.loads(config_str)

    ds = xarray.open_dataset("/tmp/nc_outfile.nc")
    ds = ds.set_coords(t_var_str)
    feature_type_string = getattr(ds, "featureType")
    # fairly common to have first letter uppercase, but won't be recognized
    # in pocean.dsg classes
    if feature_type_string in {"Profile", "Timeseriesprofile",
                               "TrajectoryProfile", "Timeseries", "Trajectory"}:
        feature_type_string = (feature_type_string[0].lower() +
                               feature_type_string[1:])
    #feature_type = getattr(pocean.dsg, feature_type_string)
    xrs = XarrayStream(ds, lon=x_var_str, lat=y_var_str, z=z_var_str,
                       time=t_var_str)
    config = QcConfig(config_dict)
    results = list(xrs.run(config))
    #store = CFNetCDFStore(run_list)
    # appears to need x, y coordinates even if location test is not used
    store = PandasStore(results,
                        axes={"x": x_var_str,
                              "y": y_var_str,
                              "z": z_var_str,
                              "t": t_var_str})

    outfile_path = "/tmp/qced_results.csv"
    # feature type detection fails on certain CDM data types
    #qc_all = store.save(outfile_path, feature_type, config, write_data=True)
    qc_all = store.save(write_data=True, write_axes=True)
    qc_all.to_csv(outfile_path, index=False)

if __name__ == "__main__":
    main(*sys.argv[1:6])
