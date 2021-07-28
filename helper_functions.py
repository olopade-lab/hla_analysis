from datetime import datetime
import os
import importlib
from parsl import load as parsl_load
import exceptions

def time_stamp():
    now = datetime.now()
    return(now.strftime("%m/%d/%Y, %H:%M:%S"))

def write_log(message, out):
    with open(out + "parsl_log.txt", "a+") as log:
        log.write(time_stamp() + ":  " + message + "\n")

def load_config(config_name):
    base_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
    config = os.path.join(base_dir, 'configs', '{}.py'.format(config_name))
    if not os.path.isfile(config):
        raise exceptions.IncorrectPathError("Cannot find the config file <{config}.py>.".format(config=config_name))
    try:
        spec = importlib.util.spec_from_file_location('', config)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        parsl_load(module.config)
    except Exception as e:
        raise exceptions.IncorrectInputFiles(("Could not load specified config from <{config}.py> :"
                                            "\n {exception}.").format(config=config_name, exception=e))