### Google Spreadsheet Crawler
[Test for Unwind Digital]

First, you have to place your Google Service account credentials in `configs/` directory and rename it to `google_credentials.json`.
Second, provide address to deployed database in `configs/params.conf` file.
If you want to delete rows, which are missed in spreadsheet:
```
delete_removed=True
```
If you want to update currency for missed rows:
```
update_price_for_removed=True
```

All other params you can find in the same file `configs/params.conf`

Run crawler:
```
python run.py
```