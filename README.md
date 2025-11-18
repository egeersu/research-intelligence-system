## Setup

Create a virtual environment:
```bash
python3 -m venv venv
```

Activate the virtual environment:
```bash
source venv/bin/activate
```

Install dependencies:
```bash
pip3 install -r requirements.txt
```

Run the project:
```bash
python3 main.py
```
Run analysis notebook:
```bash
jupyter lab
```


## Data Model & Storage
Table schemas available
Coverage on openalex
I used DuckDB because
You can start from scratch or use ... to get 1000 days of data. (x mb)


## Scheduled Ingestion
Open alex
decisions made : recheck logic

## Query & Insight Layer
The main script supports the following methods:

## Traction & Impact
It was not possible to collect historical citation data within a week, so I'm creating a fake table with the same schema as citation snapshot, simulating how it would grow if allowed to run for a while.
The main metric is momentum.

## Linkedin Matching
Sadly it is very hard to scrape google/linkedin without using third party services. I used [Swarm](https://www.theswarm.com/product/data-api) which supports searching linkedin profiles through name and institutuions. Combining the Author's full name and institutuon coming from OpenAlex yields good results. I verify that their instutition shows up within their experience to confirm its the same person. Does not work well if their university on linkedin is in their native language or spelled differently - that would require finding the native spelling of the uni + fuzzy matching. 

## Expert Graph
Sadly no time left, but I'd have...