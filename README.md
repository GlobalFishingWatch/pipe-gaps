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

### Core gap detector

> **Note**  
> Currently, the core algorithm takes about `(1.75 ± 0.01)` seconds to process 10M messages.  
  Tested on a i7-1355U 5.0GHz processor.

#### Installation:
```shell
pip install pipe-gaps
```

#### Using from python code:
```python
from pipe_gaps.core import gap_detector as gd
from datetime import timedelta

gaps = gd.detect(messages, threshold=timedelta(hours=1, minutes=20))
```

Where `messages` is a list of AIS position messages with the format:
```python
{
    "ssvid": "226013750",
    "seg_id": "226013750-2023-01-01T00:10:37.000000Z",
    "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
    "timestamp": "2024-01-04 20:48:40.000000 UTC",
    "lat": "44.556666666666665",
    "lon": "-0.24666666666666667",
    "course": "511.0",
    "speed_knots": "0.0",
    "type": "AIS.27",
    "receiver_type": "satellite",
    "distance_from_shore_m": "0.0",
    "distance_from_port_m": "40112.82"
}
```

#### Using from CLI code:

Not yet implemented.

### Apache Beam integration

Not yet implemented.



## References
<a id="1">[1]</a> Welch H., Clavelle T., White T. D., Cimino M. A., Van Osdel J., Hochberg T., et al. (2022). Hot spots of unseen fishing vessels. Sci. Adv. 8 (44), eabq2109. doi: 10.1126/sciadv.abq2109

<a id="1">[2]</a> J. H. Ford, B. Bergseth, C. Wilcox, Chasing the fish oil—Do bunker vessels hold the key to fisheries crime networks? Front. Mar. Sci. 5, 267 (2018).