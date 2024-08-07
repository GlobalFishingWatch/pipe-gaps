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
exceeds a configured `gap_threshold` (typically 12 hours).

When the period of time between **last** known good position
and the present time exceeds the `gap_threshold`,
we call it an **opened** gap event.

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

#### Core gap detector

> **Note**  
> Currently, the core algorithm takes about `(1.75 ± 0.01)` seconds to process 10M messages.  
  Tested on a i7-1355U 5.0GHz processor.


```python
from pprint import pprint
from datetime import timedelta, datetime

from pipe_gaps.core import gap_detector as gd

messages = [
    {
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": datetime(2024, 1, 1, 0).timestamp(),
        "distance_from_shore_m": 1
    },
    {
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": datetime(2024, 1, 1, 1).timestamp(),
        "distance_from_shore_m": 1
    }
]

gaps = gd.detect(messages, threshold=timedelta(hours=0, minutes=50), show_progress=True)
pprint(gaps)
```

#### BigQuery integration

First, authenticate to bigquery and configure project:
```shell
docker compose run gcloud auth application-default login
docker compose run gcloud config set project world-fishing-827
docker compose run gcloud auth application-default set-quota-project world-fishing-827
```

Then you can do:
```python
from datetime import timedelta, datetime

from pipe_gaps import pipeline
from pipe_gaps.utils import setup_logger

setup_logger()

config = dict(
    input_query=dict(
        start_date=datetime(2019, 2, 1).date(),
        end_date=datetime(2019, 2, 2).date(),
        ssvids=[412331104, 477334300]
    ),
    core=dict(
        threshold=timedelta(hours=0.1),
        show_progress=True
    ),
    save_json=True
)

pipe = pipeline.create(**config)
pipe.run()
```


### Using from CLI:

```shell
(.venv) $ pipe-gaps
usage: pipe-gaps [-h] [-c ] [-i ] [--pipe-type ] [--save-json] [--work-dir ] [-v] [--threshold ] [--show-progress] [--start-date ] [--end-date ] [--ssvids   [ ...]] [--mock-db-client]

    Detects time gaps in AIS position messages.
    The definition of a gap is configurable by a time threshold.

    If input-file is provided, all query parameters are ignored.

options:
  -h, --help             show this help message and exit
  -c  , --config-file    JSON file with pipeline configuration (default: None).
  -i  , --input-file     JSON file with input messages to use.
  --pipe-type            Pipeline type: ['naive', 'beam'].
  --save-json            If passed, saves the results in JSON file.
  --work-dir             Directory to use as working directory.
  -v, --verbose          Set logger level to DEBUG.

core algorithm:
  --threshold            Minimum time difference (hours) to start considering gaps.
  --show-progress        If passed, renders a progress bar.

query parameters:
  --start-date           Query filter: messages after this dete, e.g., '2024-01-01'.
  --end-date             Query filter: messages before this date, e.g., '2024-01-02'.
  --ssvids   [  ...]     Query filter: list of ssvids.
  --mock-db-client       If passed, mocks the DB client. Useful for development and testing.

Examples: 
    pipe-gaps --start-date 2019-01-02 --end-date 2019-01-03 --threshold 0.1 --ssvids 412331104 477334300
    pipe-gaps -c config/sample-from-file-1.json --threshold 1.3
```
### Apache Beam integration

Just use `--pipe-type beam` option:
```
pipe-gaps --pipe-type beam --start-date 2019-01-02 --end-date 2019-01-03 --threshold 0.1 --ssvids 412331104 477334300
```
This will run by default with DirectRunner.
To run on DataFlow, add `--runner dataflow` option.

Beam integrated pipeline will parallelize grouping inputs by SSVID and year.
Border cases between years and open gaps are not handled in this version.


## References
<a id="1">[1]</a> Welch H., Clavelle T., White T. D., Cimino M. A., Van Osdel J., Hochberg T., et al. (2022). Hot spots of unseen fishing vessels. Sci. Adv. 8 (44), eabq2109. doi: 10.1126/sciadv.abq2109

<a id="1">[2]</a> J. H. Ford, B. Bergseth, C. Wilcox, Chasing the fish oil—Do bunker vessels hold the key to fisheries crime networks? Front. Mar. Sci. 5, 267 (2018).