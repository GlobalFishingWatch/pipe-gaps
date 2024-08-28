from pipe_gaps import pipeline
from pipe_gaps.utils import setup_logger

setup_logger()

pipe_config = {
    "inputs": [
        {
            "kind": "query",
            "query_name": "messages",
            "query_params": {
                "source_messages": "pipe_ais_v3_published.messages",
                "source_segments": "pipe_ais_v3_published.segs_activity",
                "start_date": "2024-01-01",
                "end_date": "2024-01-02",
                "ssvids": [412331104, 477334300]
            },
            "mock_db_client": False
        }
    ],
    "core": {
        "threshold": 1,
        "show_progress": False,
        "eval_last": True
    },
    "options": {
        "runner": "direct",
        "region": "us-east1",
        "network": "gfw-internal-network",
        "subnetwork": "regions/us-east1/subnetworks/gfw-internal-us-east1"
    },
    "save_json": True
}


pipe = pipeline.create(pipe_type="beam", **pipe_config)
pipe.run()
