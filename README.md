# qs

## 1) Equity Tick Data

### Liquidity Flow Data:
- grouped by time-bucket & price
- liquidity add: add_bid_size & add_ask_size
- liquidity take: take_buy_size & take_ask_size
  
<img width="724" alt="image" src="https://github.com/niyoh/qs/assets/6595331/56252075-c036-4ed5-bd38-45de2b607e6a">

### Summary Data:
- grouped by time-bucket
- open & high & low & close, etc

<img width="1438" alt="image" src="https://github.com/niyoh/qs/assets/6595331/06928c58-35a7-44c7-b5c2-f463dc2f35d3">

## 2) Continuous Futures

### Rank by Nearest Expiry (IF futures, IFc1/2/3):
(green line for adjusted price, grey line for original price. marked roll dates by red triangles)

<img width="1250" alt="image" src="https://github.com/niyoh/qs/assets/6595331/2a6e70a8-c1fc-484d-abb9-6e004a9771fd">


### Rank by Most Active (P futures, Pv1/2/3):
(green line for adjusted price, grey line for original price. marked roll dates by red triangles)

<img width="1301" alt="image" src="https://github.com/niyoh/qs/assets/6595331/00cac2af-c480-4b57-aff5-1956b7543d59">


## 3) US Treasury Auctions

- Scrape data from JSON API
- Stored in sqlite.
- Scheduled in docker-ized Airflow
- Hosted in EC2.

<img width="1370" alt="image" src="https://github.com/niyoh/qs/assets/6595331/780b2bcc-b0fd-462c-89f7-64402030ac3e">

Schema design:

- Tables for Auctioned Results (auctioned) and Upcoming Auctions (upcoming)
- Append-only design: retain track record / flexibility for any auction changes
- Results are unique by version (write timestamp) + type (product type) + cusip
- Blob column records full JSON, is added to store less important info 

<img width="1012" alt="image" src="https://github.com/niyoh/qs/assets/6595331/ef384270-30fd-4197-b9ce-2aa1a2979ca9">


Setup doc:
- docker-compose.yaml in this repository
- in EC2 host's home directory:
-   ~/airflow-docker is root folder for airflow configs and outputs
-   ~/airflow-docker/dags is a sym-link to ~/qs/dags (i.e. ~/qs is the cloned git repository, for easier deployment)
-   ~/airflow-docker/scraper stores sqlite database, mounted to container's /tmp/scraper. container writes data to /tmp/scraper which is physical host's mount point for data persistence, even after container restart


