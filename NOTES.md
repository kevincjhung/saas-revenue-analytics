# Notes


## Miniforge/Conda

✅ Step 1: Create a New Environment

```conda create -n salespipeline python=3.13 pandas```

-n salespipeline: the name of the environment
python=3.13 pandas: installs Python 3.13 and pandas into it


✅ Step 2: Activate the Environment

```conda activate salespipeline```
Your terminal prompt will now look like this:

✅ Step 3: Run your script
Make sure you're in your project folder, then run:

```python main.py```

It will now use the Python and pandas from your finance environment.

✅ Step 4: Add More Packages (Optional)

```conda install matplotlib```

Or for non-Conda packages:

```pip install openpyxl```

Yes, you can use pip inside a conda environment — just try to prefer conda if the package exists there.

Deactivating the Environment
```conda deactivate```

List all environments
```conda env list``` or ```conda info --envs```

Remove environment
```conda remove -n salespipeline --all```

Export environment file	
```conda env export > environment.yml```

Recreate from file	
```conda env create -f environment.yml```

## Testing

```bash 
  python -m pytest -v
```

## Database

Initialize database
```python -m salespipeline.db.init_db```

Create & Insert Accounts
```bash
  python -m salespipeline.db.data_loading.load_accounts
```   

```bash 
  python -m salespipeline.db.data_loading.load_leads
```   

```bash
  python -m salespipeline.db.data_loading.load_contacts
```

```bash
  python -m salespipeline.db.data_loading.load_opportunities
``` 




## Data model

```sql

-- organizations and companies you do business with
Table accounts { 
  account_id uuid [pk, note: 'Unique identifier for the account/company']
  name varchar(255) [not null, note: 'Company name']
  industry varchar(100) [note: 'Industry classification']
  annual_revenue numeric(15,2) [note: 'Estimated annual revenue']
  region varchar(100) [note: 'Geographical region or sales territory']
  created_at timestamp [default: `now()`, note: 'Record creation timestamp']
}

-- raw inbound/outbound prospects. leads table append only, even if lead progresses to Opportunity stage
Table leads {
  lead_id uuid [pk, note: 'Primary key for lead record']
  created_at timestamp [not null, default: `now()`]
  lead_source varchar(50) [note: 'web, paid_search, events, referral, partner, etc.']
  owner_id uuid [note: 'Sales rep responsible for this lead']
  email varchar(255)
  account_id uuid [ref: > accounts.account_id, note: 'associates lead with company if known']
  is_marketing_qualified boolean [note: 'True if lead reached MQL stage']
}

-- individuals at an account engaged in conversation. Typically converted leads 
Table contacts {
  contact_id uuid [pk, note: 'Primary key for contact record']
  lead_id uuid [ref: > leads.lead_id, note: 'Original lead record']
  account_id uuid [ref: > accounts.account_id, note: 'Associated company']
  created_at timestamp [not null, default: `now()`]
  email varchar(255)
  title varchar(100)
  geo varchar(100) [note: 'Geographical location or region']
}

-- potential deals tied to accounts. Each opportunity has associated value and probability
Table opportunities {
  opportunity_id uuid [pk, note: 'Unique identifier for opportunity']
  account_id uuid [not null, ref: > accounts.account_id]
  owner_id uuid [not null, note: 'Sales rep responsible for this opportunity']
  created_at timestamp [default: `now()`]
  close_date timestamp
  amount numeric(15,2) [note: 'Deal value']
  currency varchar(10) [default: 'USD']
  stage varchar(100) [note: 'Current sales stage']
  stage_probability numeric(5,2) [note: 'System or manually assigned probability (0–1)']
  lead_source varchar(50)
  campaign_id uuid [note: 'Optional reference to marketing campaign']
  is_closed boolean [default: false]
  close_outcome varchar(50) [note: 'closed_won / closed_lost / disqualified']
  product_line varchar(100)
}

-- historical transitions of each opportunity through the stages
Table opportunity_stage_history {
  stage_history_id uuid [pk, note: 'Primary key for stage history record']
  opportunity_id uuid [not null, ref: > opportunities.opportunity_id] 
  stage_name varchar(100) -- TODO: enum all stages
  entered_at timestamp [not null, note: 'Timestamp when stage was entered']
  changed_by uuid [note: 'User ID of person who updated stage']
  notes text
}

-- tracks sales interactions (emails, calls, demos) tied to opportunities
Table activities {
  activity_id uuid [pk, note: 'Primary key for sales activity record']
  opportunity_id uuid [ref: > opportunities.opportunity_id]
  contact_id uuid [ref: > contacts.contact_id]
  activity_type varchar(50) [note: 'email, call, meeting, demo']
  occurred_at timestamp [not null, note: 'When the activity occurred']
  direction varchar(10) [note: 'inbound or outbound']
  duration_seconds int [note: 'Call/meeting duration if applicable']
  outcome varchar(100) [note: 'Result of activity, e.g. connected, no_show, interested']
}

-- marketing touchpoints such as form fills, ad clicks, campaign interactions. For attribution analysis
Table marketing_events {
  event_id uuid [pk, note: 'Primary key for marketing event']
  lead_id uuid [ref: > leads.lead_id]
  event_type varchar(50) [note: 'form_submit, paid_click, webinar, etc.']
  utm_source varchar(100)
  utm_campaign varchar(100)
  occurred_at timestamp [not null]
  cost numeric(10,2)
}

-- closed deals turned into billable contracts. Used for revenue recognition, ARR/MRR analytics
Table billing_orders {
  order_id uuid [pk, note: 'Primary key for billing record']
  account_id uuid [not null, ref: > accounts.account_id]
  opportunity_id uuid [ref: > opportunities.opportunity_id, note: 'Optional reference to originating deal']
  amount numeric(15,2)
  currency varchar(10) [default: 'USD']
  order_date timestamp [not null]
  term_months int [note: 'Contract length']
}
```
