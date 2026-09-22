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
11. What were our summer sales in 2025?
12. How many customers do we have?

## Business conventions that came with the questions

These arrived WITH the question list, from the business, and are part of the
material an answer is judged against. None of them is expressed anywhere in
`storefront.malloy`, which is the point: an agent cannot infer a convention
the model does not encode, however well documented the model's fields are.

- **Summer** is the company's sales season, **25 May to 15 September
  inclusive**, not the calendar or meteorological summer. It is set by the
  buying calendar and does not move.
- **Net** of cancellations and returns means excluding lines with status
  `Cancelled` or `Returned`. Gross includes them.

## A question with two right answers

Question 12 is here because the model does not resolve it and neither does the
business. "Customers" can mean the 1,000 people on file or the 974 who have
placed at least one order, and both are defensible. The set does not pick one:
it judges whether the answer SAYS which one it used.
