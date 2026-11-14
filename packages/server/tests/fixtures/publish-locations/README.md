# Publish-location fixtures

Packages for `tests/integration/package_versions/publish_locations.integration.spec.ts`, which publishes each of them from every kind of location a publish is sent (a local folder or `.zip`, a `gs://` or `s3://` `.zip`, and a Git repository or a folder of one) and checks what each publish answers.

| Folder                      | `version` | `numbers -> which` answers | Used for                                       |
| --------------------------- | --------- | -------------------------- | ---------------------------------------------- |
| `sales-1.0.0`               | `1.0.0`   | 1                          | the first publish of a version                 |
| `sales-1.0.0-changed`       | `1.0.0`   | 2                          | other content under that version (409)         |
| `sales-unversioned`         | none      | 3                          | a publish with no version (installed in place) |
| `sales-unversioned-changed` | none      | 4                          | the next one, which replaces it                |

`zips/` holds each folder zipped with its files at the archive's root, plus `sales-1.0.0-repacked.zip`: the same files as `sales-1.0.0.zip` with other timestamps, so the archive differs while its content does not (a re-publish of it is placement, 200). The zips are committed so the tests need no `zip` tool, which Windows lacks. After changing a folder, rebuild them with:

```bash
python3 packages/server/tests/fixtures/publish-locations/make_zips.py
```

The test serves the zips from in-memory GCS and S3 buckets and the folders from a faked clone, so it needs no network or credentials.
