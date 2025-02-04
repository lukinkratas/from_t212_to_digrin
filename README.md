# From Trading212 to Digrin Csv Convertor App

- Python

- Streamlit

## Install

```
python3 -m venv venv # optional
source venv/bin/activate # optional
python3 -m pip install -r requirements.txt
echo "T212_API_KEY=$T212_API_KEY" >> .env
streamlit run app.py
```

# TODO

- app.py

    - [ ] merge - check row by row if already present

    - [ ] archive reports in parquet

    - [ ] set dtypes

    - [ ] save files to aws

    - [ ] remove s3fs dep in s3_put_df

    - [ ] add reading from s3
