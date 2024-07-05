import pytest
import cpuinfo

from pipe_gaps import benchmark


def test_measurement(tmp_path):
    measurements = [1.2, 2.3]
    measurement = benchmark.Measurement(measurements, input_size=100)

    path = tmp_path.joinpath("test_data.json")
    measurement.save(path)

    measurement = benchmark.Measurement.from_path(path)

    assert measurement.measurements == measurements

    benchmark.stats(path)


def test_run_benchmark(tmp_path):
    measurement = benchmark.run_benchmark(input_size=100, reps=2, output_dir=tmp_path)

    assert measurement.input_size == 100
    assert len(measurement.measurements) == 2


def test_cli(monkeypatch, tmp_path):
    monkeypatch.setattr(cpuinfo, "get_cpu_info", lambda *x, **y: dict(brand_raw=""))

    benchmark.main([
        'run',
        '-n', str(10),
        '--reps', str(2),
        '--output_dir', str(tmp_path)
    ])

    measurements = [1.2, 2.3]
    measurement = benchmark.Measurement(measurements, input_size=100)
    path = tmp_path.joinpath("test_data.json")
    measurement.save(path)

    benchmark.main([
        'stats',
        '-f', str(path),
    ])

    with pytest.raises(SystemExit):
        benchmark.main(['invaid_command'])
