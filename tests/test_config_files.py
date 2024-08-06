import glob
import json

from pipe_gaps.utils import json_load
from pipe_gaps.pipeline import factory


from pydantic import ValidationError


def test_config_files():
    for file in glob.glob("config/*.json"):
        try:
            config_dict = json_load(file)
            config = factory.PipelineFactoryConfig.model_validate(config_dict)
            config.pipe_config.validate()
        except (json.decoder.JSONDecodeError, ValidationError) as e:
            raise Exception(f"Error in {file}: {e}")
