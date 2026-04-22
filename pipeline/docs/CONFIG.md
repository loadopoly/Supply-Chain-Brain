# `config/brain.yaml` reference

The YAML file is the **only** thing you should ever need to edit when
onboarding a new database, adding a new table mapping, swapping a graph
backend, or registering a new cross-app subscriber.

```yaml
# ----- pluggable databases -----
connectors:
  azure_sql:
    kind: azure_sql                   # azure_sql | oracle_fusion | http_api
    server: edap-replica-cms-sqldb.database.windows.net
    database: edap_replica
    auth: ActiveDirectoryInteractive  # MFA-friendly
  oracle_fusion:
    kind: oracle_fusion
    pod_url: https://dev13.fa.us2.oraclecloud.com
    user_env: ORA_FUSION_USER
    pass_env: ORA_FUSION_PASS
  ips_freight:
    kind: http_api
    base_url: https://ips-freight-api.onrender.com
    api_key_env: IPS_FREIGHT_API_KEY

# ----- physical → logical column mapping -----
columns:
  part:        ["part_number", "item_id", "sku"]
  supplier:    ["supplier_key", "vendor_id"]
  lead_time:   ["lead_time_days", "lt_days"]
  on_hand:     ["on_hand_qty", "qty_on_hand"]
  open_qty:    ["open_qty", "open_quantity"]
  demand:      ["shipped_qty", "demand_units"]
  periods:     ["period_count", "periods"]
  unit_cost:   ["unit_cost", "std_cost"]
  promised:    ["promise_date", "promised_date"]
  received:    ["received_date", "actual_receipt_date"]

# ----- EOQ defaults -----
eoq:
  ordering_cost_default: 75.0
  holding_rate_default:  0.22
  bayes_prior_alpha:     2.0
  bayes_prior_beta:      1.0

# ----- OTD recursive clustering defaults -----
otd:
  min_n: 50
  entropy_threshold: 0.45
  max_depth: 3

# ----- pluggable graph backend -----
graph:
  backend: networkx                 # networkx | neo4j | cosmos_gremlin
  neo4j:
    uri: bolt://neo4j.example.com:7687
    user: neo4j
    password_env: NEO4J_PASSWORD
    database: neo4j
  cosmos_gremlin:
    endpoint: wss://acct.gremlin.cosmos.azure.com:443/
    database: scbrain
    graph: scbrain
    key_env: COSMOS_GREMLIN_KEY

# ----- GLEC freight emission factors (g CO2e per tonne-km) -----
emissions:
  factors:
    truck_ftl: 62
    truck_ltl: 95
    rail:      22
    ocean:     8
    air:       602

# ----- multi-echelon defaults (Graves-Willems) -----
multi_echelon:
  service_level: 0.95
  holding_rate:  0.22

# ----- HMAC-signed cross-app webhooks -----
cross_app:
  subscribers:
    - name: ips_freight
      url:  https://ips-freight-api.onrender.com/webhook
      secret_env: IPS_FREIGHT_SHARED_SECRET
      events:    [eoq.deviation, otd.cluster, freight.ghost_lane]

# ----- session identity (opt-in) -----
auth:
  enabled: false                    # toggle to true to surface picker
  default_user: anonymous
```

## Adding a new database

1. Append a `connectors:` entry (see above).
2. Add a tiny driver in `src/connections/` if `kind` is new (`azure_sql`,
   `oracle_fusion`, `http_api` are already wired).
3. Restart the Streamlit app — the connector appears in the **Connectors**
   page and can be queried via `data_access.query_df("<name>", sql)`.

## Adding a new column mapping

The pages call `schema_introspect.find_column(cols, patterns)` so a missing
physical name only means "extend the patterns list in `brain.yaml → columns`".
This avoids code edits when an upstream system renames a column.
