<h1 align="center" style="border-bottom: none;"> pipe-gaps </h1>

<p align="center">
  <a href="https://codecov.io/gh/GlobalFishingWatch/pipe-gaps">
    <img alt="Coverage" src="https://codecov.io/gh/GlobalFishingWatch/pipe-gaps/branch/develop/graph/badge.svg?token=OO2L9SXVG0">
  </a>
  <a>
    <img alt="Python versions" src="https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue">
  </a>
  <a>
    <img alt="Last release" src="https://img.shields.io/github/v/release/GlobalFishingWatch/pipe-gaps">
  </a>
</p>

Time gap detector for AIS position messages.

[bigquery-emulator]: https://github.com/goccy/bigquery-emulator
[configure a SSH-key for GitHub]: https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account
[docker official instructions]: https://docs.docker.com/engine/install/
[docker compose plugin]: https://docs.docker.com/compose/install/linux/
[examples]: examples/
[git installed]: https://git-scm.com/downloads
[git workflow documentation]: GIT-WORKFLOW.md
[Makefile]: Makefile
[pip-tools]: https://pip-tools.readthedocs.io/en/stable/
[requirements.txt]: requirements.txt
[requirements/prod.in]: requirements/prod.in
[Semantic Versioning]: https://semver.org

## Introduction

<div align="justify">

Not all **AIS** messages that are broadcast by vessels
are recorded by receivers for technical reasons,
such as signal interference in crowded waters,
spatial variability of terrestrial reception,
spatial and temporal variability of satellite reception,
and dropped signals as vessels move from terrestrial coverage
to areas of poor satellite reception.
So, as a result,
it’s not uncommon to see gaps in AIS data,
for hours or perhaps even days [[1]](#1). 

Other causes of **AIS** gaps are:
* The **AIS** transponder is turned off or otherwise disabled while at sea.
* The **AIS** transponder has a malfunction.
* The ships systems are powered off while the vessel is at anchor or in port.

AIS gaps detection is essential to identify 
possible *intentional disabling events*,
which can obscure illegal activities,
such as unauthorized fishing activity or
unauthorized transshipments [[1]](#1)[[2]](#2).

## Definition of gap

We define an **AIS** gap event when the period of time between
consecutive known good **AIS** positions from a single vessel,
after de-noise and de-spoofing),
exceeds a configured `threshold` (typically 12 hours).

When the period of time between **last** known good position
and the present time exceeds the `threshold`,
we call it an **open** gap event.

## Usage

### Installation

We still don't have a package in PYPI.
First, clone the repository.

Then
```shell
pip install .
```

If you want to use apache beam integration:
```shell
pip install .[beam]
```

### Using from python code

#### Gap detection core process

> **Note**  
> Currently, the core algorithm takes about `(1.75 ± 0.01)` seconds to process 10M messages.  
  Tested on a i7-1355U 5.0GHz processor.


```python
import json
from datetime import timedelta, datetime
from pipe_gaps.core import GapDetector

messages = [
    {
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": datetime(2024, 1, 1, 0).timestamp(),
        "receiver_type": "terrestrial",
        "lat": 30.5,
        "lon": 50.6,
        "distance_from_shore_m": 1.0
    },
    {
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": datetime(2024, 1, 1, 1).timestamp(),
        "receiver_type": "terrestrial",
        "lat": 30.5,
        "lon": 50.6,
        "distance_from_shore_m": 1.0
    }
]

gd = GapDetector(threshold=timedelta(hours=0, minutes=50))
gaps = gd.detect(messages)
print(json.dumps(gaps, indent=4))
```

#### BigQuery integration pipeline

First, authenticate to bigquery and configure project:
```shell
docker compose run gcloud auth application-default login
docker compose run gcloud config set project world-fishing-827
docker compose run gcloud auth application-default set-quota-project world-fishing-827
```

Then you can do:
```python
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
        "kind": "detect_gaps",
        "threshold": 1,
        "show_progress": False,
        "eval_last": True
    },
    "outputs": [
        {
            "kind": "json",
            "output_prefix": "gaps"
        }
    ],
    "options": {
        "runner": "direct",
        "region": "us-east1",
        "network": "gfw-internal-network",
        "subnetwork": "regions/us-east1/subnetworks/gfw-internal-us-east1"
    }
}

pipe = pipeline.create(pipe_type="naive", **pipe_config)
pipe.run()
```

> **Warning**  
> Date ranges are inclusive for the start date and exclusive for the end date.


### Using from CLI:

```shell
(.venv) $ pipe-gaps
usage: pipe-gaps [-h] [-c ] [--pipe-type ] [--save-stats | --no-save-stats] [--work-dir ] [-v] [--threshold ] [--sort-method ] [--show-progress | --no-show-progress]
                 [--eval-last | --no-eval-last] [--norm | --no-norm]

    Detects time gaps in AIS position messages.
    The definition of a gap is configurable by a time threshold.

options:
  -h, --help                           show this help message and exit
  -c  , --config-file                  JSON file with pipeline configuration (default: None).
  --pipe-type                          Pipeline type: ['naive', 'beam'].
  --save-stats, --no-save-stats        If passed, saves some statistics.
  --work-dir                           Directory to use as working directory.
  -v, --verbose                        Set logger level to DEBUG.

pipeline core process:
  --threshold                          Minimum time difference (hours) to start considering gaps.
  --sort-method                        Sorting algorithm.
  --show-progress, --no-show-progress  If passed, renders a progress bar.
  --eval-last, --no-eval-last          If passed, evaluates last message of each SSVID to create an open gap.
  --norm, --no-norm                    If passed, normalizes the output.

Example: 
    pipe-gaps -c config/sample-from-file-1.json --threshold 1.3
```
### Apache Beam integration

Just use `--pipe-type beam` option:
```
pipe-gaps --pipe-type beam -c config/sample-from-file-1.json --threshold 1.3
```
This will run by default with DirectRunner.
To run on DataFlow, add `--runner dataflow` option.

Beam integrated pipeline will parallelize grouping inputs by SSVID and year.

### Handle of open gaps

When an open gap is closed,
a new gap event is created. 
Two gap events will exist with the same `gap_id`,
one will be an old open gap, and the other one will be the current closed active gap.

To query all active gaps,
you will just need to query the last versions for every `gap_id`.
For example:
```sql
SELECT *
    FROM (
      SELECT
          *,
          MAX(gap_version)
              OVER (PARTITION BY gap_id)
              AS last_version,
      FROM `world-fishing-827.scratch_tomas_ttl30d.pipe_ais_gaps_filter_no_overlapping_and_short`
    )
    WHERE gap_version = last_version
```



## References
<a id="1">[1]</a> Welch H., Clavelle T., White T. D., Cimino M. A., Van Osdel J., Hochberg T., et al. (2022). Hot spots of unseen fishing vessels. Sci. Adv. 8 (44), eabq2109. doi: 10.1126/sciadv.abq2109

<a id="1">[2]</a> J. H. Ford, B. Bergseth, C. Wilcox, Chasing the fish oil—Do bunker vessels hold the key to fisheries crime networks? Front. Mar. Sci. 5, 267 (2018).