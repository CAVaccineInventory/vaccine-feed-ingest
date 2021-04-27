# <!-- <stage> <site> for <state> (e.g.,  Normalize ArcGIS for MD) -->

| Key Details |
|-|
| Resolves #<!-- ISSUE_NUM --> |
State: <!-- two-letter abbreviation like md or us --> |
Site: <!-- name of site, like vaccinespotter_org or arcgis--> |

## Notes
<!-- Share any information which would be helpful to the reviewer -->

## Data sample
<!-- copy the first several lines of output data into the codeblock below. Feel free to change the block type -->
```json
{...}
```

## Before Opening a PR
- [ ] I tested this using the CLI (e.g., `poetry run vaccine-feed-ingest <state>/<site>`)
- [ ] I ran auto-formatting: `poetry run tox -e lint-fix`
- [ ] If this is a normalizer, I validated that the latitude/longitude in the output are not flipped. In the continental US the latitude ranges from ~25 to ~50, and the longitude ranges from ~-124 to ~-65
