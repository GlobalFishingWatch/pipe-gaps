import glob
import json

from gfw.common.io import json_load


def test_config_files():
    for file in glob.glob("config/*.json"):
        try:
            _ = json_load(file)
        except (json.decoder.JSONDecodeError) as e:
            raise Exception(f"Error in {file}: {e}")
