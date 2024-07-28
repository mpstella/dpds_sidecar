## Kick off the Sync and RPC server

```text
$> poetry run server start --help
Usage: server start [OPTIONS]

Options:
  -s, --source TEXT       Source location (GCS Bucket + 'folder')  [required]
  -d, --destination TEXT  Destination location  [required]
  -d, --sleep INTEGER     Sleep duration between synchronising  [default: 60]
  -p, --port INTEGER      RPC port for server to bind  [default: 12345]
  --help                  Show this message and exit.
```

#### Example
```text
$> poetry run server start -s gs://airline-travel-source/sample_dpds -d /tmp/dpds_cache
```

---

### Run the client (this would be your library)

```text
$> python client.py ${PORT} ${FILE_WE_CARE_ABOUT}
$> # eg.
$> python client.py 1234 sales.json
```
