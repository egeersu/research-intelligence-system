You’re tasked with building a research intelligence system that continuously analyzes papers from bioRxiv or medRxiv.

Please push your work to a Git repo by next Monday, and include a README explaining your architecture, reasoning, and how to run the project. You are not expected to build a frontend for this, focus on the backend and data part of it.

You can use the following API:
https://api.biorxiv.org/details/{server}/{start_date}/{end_date}/{cursor}/json
where server can be biorxiv or medrxiv.

You are encouraged to use AI while coding, but you should be able to clearly explain every part of your solution and the trade-offs you’ve made.


Problem:

1. Scheduled Ingestion (Weekly)
Design a scheduled process (cronjob, worker, or pipeline) that runs weekly and pulls new or updated papers between the last run and “now” using the APIs and cursor-based pagination. It should work for both biorxiv and medrxiv.
The ingestion process should be incremental (no full re-scrapes), idempotent (safe to rerun), and resilient (handles errors, retries, basic logging). This part doesn’t need to be over-engineered, just clean and clear :)

2. Data Model & Storage
Store the essential metadata; title, abstract, publication date, category/subject, DOI/ID, URL, authors, and affiliations.
Support filtering by a defined Top 30 universities (implement your own canonical mapping) and handle messy or ambiguous affiliation strings.
You may also narrow the scope by category (using the categories field) if it helps reduce complexity (just document your choice). 

Use whatever storage you think is appropriate (relational DB, document store, etc.) as long as it supports the query patterns in the next section.

3. Query & Insight Layer
The most important part of this exercise is how a user would interact with the ingested data.
Design a programmatic interface (API endpoints, scripts, or notebooks) that can answer questions like: 

“Which papers discussed the topic: X, Y, or Z?”

“What topics are gaining traction over the last N weeks or months?”

“Who are the key experts/authors in topic T (e.g. obesity treatment)?”


Optional Improvements:
If you have time and want to expand on it, here are a few more ideas, you can pick and choose whatever, or come up with your own suggestions - but as I said it's optional :) 
- Traction & Impact: You can extend your system to detect whether a paper is gaining traction by tracking citations, mentions, or growth patterns over time. This could involve calling external sources (e.g. Google Scholar or SerpApi) or simulating traction signals. The goal is to surface papers that are becoming influential rather than simply recent.

- Expert Graph: Build logic to identify experts for a given topic based on their publication history, recency, and influence. You could represent this as a network or ranking system that highlights top contributors per domain. If you want to go further, model co-authorship relationships or topic clusters to uncover collaboration patterns or emerging subfields.

- Author → LinkedIn Matching: Implement or design a way to connect an author to a potential LinkedIn profile using name, affiliation, and field of work. Explain your approach — e.g. heuristic matching, AI-assisted entity resolution, or API queries, along with how you’d disambiguate similar names or institutions. It doesn’t have to be perfect; what matters is how you reason through the matching process.


If you have any questions or issues, feel free to reach out, and if you need extension, please do reach out. Good luck and looking forward to seeing what you will come up with.

Best,

Diala 

