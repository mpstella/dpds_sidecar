### Kick off the Sync and RPC server

```text
$> poetry run start ${SOURCE} ${TARGET} ${SLEEP_TIME}
$> # eg.
$> poetry run start gs://airline-travel-source/sample_dpds /home/mastella/workspace/dpds-sidecar/cache 10
```


### Run the client (this would be your library)

```text
$> python client.py ${FILE_WE_CARE_ABOUT}
$> # eg.
$> python client.py sales.json
```
