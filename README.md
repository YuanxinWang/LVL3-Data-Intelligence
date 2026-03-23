# LVL3-Data-Intelligence
One Month Training Data Intelligence Program with Lufthansa
Ultimate Goal: > Build a cloud-based, production-grade Lakehouse data platform from the ground up. Acting as a Data Engineer/Architect, we will ingest real-world operational data from Lufthansa and transform it into actionable business insights for stakeholders (e.g., flight delay metrics and route analytics).

-- Developement Notes (For myself to keep track only) ---
note for later: 
for now config in one file to secure single source of truth - better for rather smaller pipeline
care for single case: return a list instead of a dictionary
|^ solution: manually add back the braket - fetch only english names

version control: Try my best to not repeat myself
- keep functions small and simple. One job one function.
api control:
- reference data - limit 100. flight status - limit 50
- Prcess error: if first page - assume no data. if not, meaning within total count, meaning broken data inside, throw to binary search to find broken data, skip
- other error: In case too many error cases to be caught - use "meta" key to detect "fake 200" errors. exponential backoff strategy retry - afterwards give up
- for known issue of API: when single data, return dictionary instead of list. In this case, manually detect and add [] to force into a list
- another known issue of API: first page duplication. In our case, meaning: on edge case (data total count % 100 == 0), last piece of data will be lost. No good solution so far.
goal: no extra column & no rescure data

next step:
- star schema, SCD Type 1.

new issue in silver: Metaspace OOM
problem: when trying to explode JSON, drop dup & na, and join table, crush.
solution: add staging layer, first only wash data -> 5 tmp silver table, then join. -> still crush
new solution: seperate into 2 different job tasks. Manually add action barrier
new problem: when joining, all three table have their own timestamp. crush.
new solution: Data Lineage: new table, new stamp.

idea: Golden rules: always store UTC time - in our case, to avoid chaos for international flights with jetlag & DST issues. Later in Gold layer, transform back to europe time.
YAGNI: You Aren't Gonna Need It we dropped marketing carrier infor at this stage - without sales data, this one is a bit useless.
for flight status code, API did provided status code dic in documentation - I don't trust them anymore.
Since the defination is anyway included in returned JSON, we will keep both into our table - Future-Proof. Also, storage is cheap. A few letters more doesn't hurt that much in compare.

Great. Flight status OOM again.
Split into multiple steps. Broke again - timestamp not standard (only until minutes, no seconds)
cast more specifically

I start to add more detailed remark on functions: looks stupid but too many new things to learn I'm afraid I will forget some at one point and won't understand my own code.

Great to know: in flight status, it's called "airline id", but in reference_airline, it's called "airline code"
New issue: did not seperate departure and arrival data. Now the data tables are messy. Need messive update
