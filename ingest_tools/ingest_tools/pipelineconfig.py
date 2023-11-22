from dataclasses import dataclass
import typing


@dataclass
class KerchunkPipelineConfig():

    dest_bucket: str
    dest_prefix: str
    fileformat: str
    filters: typing.List[str]    


@dataclass
class PipelineConfig():

    filters: typing.List[str]
    concat_dims: typing.List[str]
    identical_dims: typing.List[str]       


## TODO: Maybe singleton?
class ConfigContext():

    def __init__(self) -> None:
        # TODO: These can be moved out to files

        nos_kerchunk = KerchunkPipelineConfig(
            "s3://figure-this-out", 'nos', '.nc', 
            ['cbofs', 'ciofs', 'creofs', 'dbofs', 'gomofs', 'leofs', 'lmhofs', 'loofs', 'lsofs', 'ngofs2', 'sfbofs', 'tbofs', 'wcofs'])
        
        rtofs_kerchunk = KerchunkPipelineConfig(
            "s3://dest_bucket", 'rtofs', '.nc', ['rtofs']
        )

        roms = PipelineConfig(["cbofs", "ciofs", "dbofs", 'gomofs', "tbofs", "wcofs"],
                                    concat_dims = ['ocean_time'],
                                    identical_dims = [
                                            'eta_rho', 
                                            'xi_rho', 
                                            's_rho', 
                                            'eta_psi', 
                                            'xi_psi', 
                                            's_w', 
                                            'eta_u', 
                                            'xi_u', 
                                            'eta_v', 
                                            'xi_v', 
                                            'lat_rho', 
                                            'lat_psi', 
                                            'lat_u', 
                                            'lat_v', 
                                            'lon_rho', 
                                            'lon_psi', 
                                            'lon_u', 
                                            'lon_v'
                                        ])
        fvcom = PipelineConfig(["leofs", "lmhofs", "loofs", 'lsofs', "ngofs2", "sfbofs"],
                                    concat_dims=['time'],
                                    identical_dims=['lon', 'lat', 'lonc', 'latc', 'siglay', 'siglev', 'nele', 'node'])
        selfe = PipelineConfig(['creofs'], concat_dims=['time'], identical_dims=['lon', 'lat', 'sigma'])
        rtofs_config = PipelineConfig(['rtofs'], ['MT'], ['Y', 'X', 'Latitude', 'Longitude'])
        self.configs = {  
            'nos_kerchunk': nos_kerchunk,  
            'rtofs_kerchunk': rtofs_kerchunk,            
            'roms': roms,
            'fvcom': fvcom,
            'selfe': selfe,
            'rtofs': rtofs_config
        }
    
    def get_config(self, name: str) -> PipelineConfig:
        if name in self.configs:
            return self.configs[name]
        return None