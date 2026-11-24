// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// ADAPT: a worked data app on the kit, over an online store's orders: the materialized `sales` source in reference/app-frame.md, "Make it fast". Keep its shape,
// replace everything in it: the brief, the config, the pages. reference/app-frame.md is the kit's reference and
// reference/data-app-design.md says what each page and block is for.
//
// Sales performance: how the store is selling, for whoever runs it, opened weekly.
// Brief: the reader decides where to push (categories, brands, channels) and checks whether the latest weeks beat
// the weeks before. Net revenue leads: the model's total_sales counts cancelled and returned items too.
// Pages: Overview (the subject and where it comes from), Products (categories and items), Customers (who buys and
// how they arrived); a page for each category, brand and customer, reached from any list that names one.
import { dataApp, subject, rank, share, trend, table, entityHead, pageHead, blocks, S } from './kit/kit.js'

const HEAD = ['net_sales', 'order_count', 'aov', 'user_count', 'margin_pct', 'return_rate']
const product = (s) => S.run('{ group_by: product is inventory_items.product_name, brand is inventory_items.product_brand, category is inventory_items.product_category; aggregate: net_sales, items, margin_pct, return_rate; order_by: net_sales desc; limit: 300 }', s)
const productCols = [
  { key: 'product', label: 'Product' }, { key: 'brand', label: 'Brand' }, { key: 'category', label: 'Category' },
  { key: 'net_sales', metric: 'net_sales', bar: true }, { key: 'items', metric: 'items' },
  { key: 'margin_pct', metric: 'margin_pct' }, { key: 'return_rate', metric: 'return_rate' },
]

dataApp({
  title: 'Sales performance',
  sub: 'Online store',
  theme: 'bloom',
  model: 'sales.malloy',
  source: 'sales',
  date: { field: 'order_date', label: 'Order date' },
  recordNoun: 'order lines',
  metrics: {
    net_sales: { label: 'Net revenue', format: 'usd', good: 'up' },
    order_count: { label: 'Orders', format: 'number', good: 'up' },
    aov: { label: 'Average order', format: 'usd', good: 'up' },
    user_count: { label: 'Customers', format: 'number', good: 'up' },
    margin_pct: { label: 'Gross margin', format: 'percent', good: 'up' },
    return_rate: { label: 'Return rate', format: 'percent', good: 'down' },
    items: { label: 'Items sold', format: 'number', good: 'up' },
  },
  filters: [
    { key: 'department', field: 'department', label: 'Department', plural: 'departments' },
    { key: 'channel', field: 'channel', label: 'Channel', plural: 'channels' },
    { key: 'country', field: 'country', label: 'Country', plural: 'countries' },
    { key: 'state', field: 'state', label: 'State or region', plural: 'states and regions' },
    { key: 'status', field: 'status', label: 'Order status', plural: 'statuses' },
  ],
  entities: {
    category: { field: 'category', label: 'Category', plural: 'categories' },
    brand: { field: 'brand', label: 'Brand', plural: 'brands' },
    customer: { field: 'user_id', label: 'Customer', plural: 'customers', nameField: 'users.full_name' },
  },
  defaults: { period: 'last_91', compare: 'previous' },
  pages: [
    { id: 'overview', title: 'Overview', render: overview },
    { id: 'products', title: 'Products', lists: 'category', render: products },
    { id: 'customers', title: 'Customers', lists: 'customer', render: customers },
    { id: 'category', title: 'Category', entity: 'category', render: category },
    { id: 'brand', title: 'Brand', entity: 'brand', render: brand },
    { id: 'customer', title: 'Customer', entity: 'customer', render: customer },
  ],
  footer: 'Order lines from the store’s order system. Net revenue leaves out cancelled and returned items.',
})

function overview() {
  return blocks(
    subject({ metrics: HEAD }),
    rank({ entity: 'category', metric: 'net_sales', top: 10 }),
    share({ field: 'channel', metric: 'net_sales', kicker: 'Net revenue by channel · how customers first found the store' }),
    rank({ entity: 'brand', metric: 'net_sales', top: 10 }),
    rank({ field: 'state', metric: 'net_sales', top: 10, kicker: 'Net revenue by state or region' }),
    trend({ metric: 'net_sales', by: 'category', top: 5, kicker: 'Net revenue over time, the five largest categories' }),
  )
}

function products() {
  return [
    pageHead('Products', 'Which categories and items earn the most, at what margin, and which come back.'),
    blocks(
      rank({ entity: 'category', metric: 'net_sales', top: 26, extra: ['margin_pct'], span: 12, kicker: 'Every category by net revenue' }),
      rank({ entity: 'category', metric: 'margin_pct', top: 10, kicker: 'Gross margin by category, highest first' }),
      rank({ entity: 'category', metric: 'return_rate', top: 10, kicker: 'Return rate by category, highest first' }),
      table({ kicker: 'Top products', query: product, columns: productCols, sort: { key: 'net_sales', desc: true } }),
    ),
  ]
}

function customers() {
  return [
    pageHead('Customers', 'Who buys, how they found the store, and how much they are worth over time.'),
    blocks(
      subject({ metrics: ['user_count', 'aov', 'order_count'], layout: 'board' }),
      share({ field: 'channel', metric: 'user_count', kicker: 'Customers by channel' }),
      rank({ field: 'user_order_facts.spend_category', metric: 'net_sales', top: 4, kicker: 'Net revenue by lifetime spend', headline: (rows) => [{ b: rows[0].spend_category }, ` customers bring the most net revenue; the four tiers are set by what each customer has spent, all time.`] }),
      table({
        kicker: 'Top customers in the period', open: { entity: 'customer', key: 'user_id' }, sort: { key: 'net_sales', desc: true },
        query: (s) => S.run('{ group_by: user_id, customer is users.full_name, state is users.state, channel is users.traffic_source; aggregate: net_sales, order_count, aov; order_by: net_sales desc; limit: 200 }', s),
        columns: [{ key: 'customer', label: 'Customer' }, { key: 'state', label: 'State' }, { key: 'channel', label: 'Channel' }, { key: 'net_sales', metric: 'net_sales', bar: true }, { key: 'order_count', metric: 'order_count' }, { key: 'aov', metric: 'aov' }],
      }),
    ),
  ]
}

function category() {
  return [
    entityHead({ metric: 'net_sales' }),
    blocks(
      subject({ metrics: ['net_sales', 'items', 'margin_pct', 'return_rate'] }),
      rank({ entity: 'brand', metric: 'net_sales', top: 10, kicker: 'Its brands by net revenue' }),
      share({ field: 'department', metric: 'net_sales', kicker: 'Net revenue by department' }),
      table({ kicker: 'Its products', query: product, columns: productCols.filter((c) => c.key !== 'category'), sort: { key: 'net_sales', desc: true } }),
    ),
  ]
}

function brand() {
  return [
    entityHead({ metric: 'net_sales' }),
    blocks(
      subject({ metrics: ['net_sales', 'items', 'margin_pct', 'return_rate'] }),
      rank({ entity: 'category', metric: 'net_sales', top: 10, kicker: 'Its categories by net revenue' }),
      share({ field: 'channel', metric: 'net_sales', kicker: 'Its buyers by channel' }),
      table({ kicker: 'Its products', query: product, columns: productCols.filter((c) => c.key !== 'brand'), sort: { key: 'net_sales', desc: true } }),
    ),
  ]
}

function customer() {
  return [
    entityHead({ metric: 'net_sales' }),
    blocks(
      rank({ entity: 'category', metric: 'net_sales', top: 8, kicker: 'What they buy', headline: null }),
      rank({ entity: 'brand', metric: 'net_sales', top: 8, kicker: 'Their brands', headline: null }),
      table({
        kicker: 'Their order lines in the period', sort: { key: 'created_at', desc: true },
        query: (s) => S.run('{ select: created_at, order_id, product is inventory_items.product_name, category is inventory_items.product_category, status, sale_price; order_by: created_at desc; limit: 500 }', s),
        columns: [{ key: 'created_at', label: 'Ordered', format: (v) => String(v).slice(0, 10) }, { key: 'product', label: 'Product' }, { key: 'category', label: 'Category' }, { key: 'status', label: 'Status' }, { key: 'sale_price', label: 'Price', format: 'usd' }],
      }),
    ),
  ]
}
