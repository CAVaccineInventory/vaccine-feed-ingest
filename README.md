# Vaccine Feed Ingest

## Quickstart

```bash
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

## Run

Run the Vaccine Spotter crawler:

```bash
mkdir -p out/CA/vaccinespotter/raw
python3 runners/CA/vaccinespotter.py --raw-output-dir=out/CA/vaccinespotter/raw --ndjson-output-file=out/CA/vaccinespotter/locations.ndjson
```
