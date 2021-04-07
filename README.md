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
mkdir -p out/CA/vaccinespotter
python3 runners/CA/vaccinespotter/run.py --output-dir=out/CA/vaccinespotter
```
