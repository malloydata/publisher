# storefront-tour — the questions

Ten natural-language questions over the bundled `storefront` package, authored
by hand for the eval-loop walkthrough. Chosen to vary grain, join depth, filter
shape and answerability, and deliberately NOT all answerable from the model as
it stands: three of them ask for a concept `storefront.malloy` does not define,
which is what gives the run something to diagnose.

Question text below is the source of truth. `cases.jsonl` copies it byte for
byte and seals it with `questionSha`.

1. Which product category brings in the most revenue, and how much?
2. Which brand sells at the highest margin rate?
3. Which month of 2025 had the highest revenue?
4. How many orders does the average customer place?
5. Who is our single biggest customer by total spend?
6. What were net sales in 2025, excluding cancelled and returned items?
7. How much revenue came from customers who signed up in 2025?
8. On average, how far below list price do we actually sell?
9. Which sales region brings in the most revenue?
10. Who are our best customers?
