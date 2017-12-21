# Fasecolda DBs Scraper

This scripts provide total functionality for scraping data from the fasecolda website and updating
the databases on Matchcar's platform with the required format.

Firefox must be installed on the server.

## Folder structure for storing data

The following structure of folders must be in place in order to properly store the raw files:

```
data
|- docs
|_ files
```

The paths dictionary points to these directories. 

## Requirements for implementation

1. The guides table must be in place with the following structure:

```
                                      Table "public.guides"
   Column    |            Type             |                      Modifiers
-------------+-----------------------------+-----------------------------------------------------
 id          | integer                     | not null default nextval('guides_id_seq'::regclass)
 year_guide  | integer                     |
 month_guide | integer                     |
 month_sold  | integer                     |
 created_at  | timestamp without time zone | not null
 updated_at  | timestamp without time zone | not null
 reference   | integer                     |
Indexes:
    "guides_pkey" PRIMARY KEY, btree (id)
    "unique_reference" UNIQUE CONSTRAINT, btree (reference)
Referenced by:
    TABLE "monthly_prices" CONSTRAINT "fk_rails_7e9fd5c8af" FOREIGN KEY (guide_id) REFERENCES guides(id)
    TABLE "price_variations" CONSTRAINT "id_guide_constraint" FOREIGN KEY (id_guide_pk) REFERENCES guides(id)
```

Notes:
* Add a new unique field references 
* Removed the model_year from the reference.
* The important field is month_sold


2. The guides table must be in place with the following structure

```
                                           Table "public.price_variations"
        Column         |            Type             |                           Modifiers
-----------------------+-----------------------------+---------------------------------------------------------------
 id                    | integer                     | not null default nextval('price_variations_id_seq'::regclass)
 yearly_price_id       | integer                     |
 market_price          | numeric                     | not null
 max_price_percentage  | double precision            | not null
 min_price_percentage  | double precision            | not null
 med_price_percentage  | double precision            | not null
 good_price_percentage | double precision            | not null
 max_level             | double precision            | default 0.05
 min_level             | double precision            | default 0.05
 created_at            | timestamp without time zone | not null
 updated_at            | timestamp without time zone | not null
 id_guide_pk           | integer                     |
Indexes:
    "price_variations_pkey" PRIMARY KEY, btree (id)
    "index_price_variations_on_yearly_price_id" btree (yearly_price_id)
Foreign-key constraints:
    "id_guide_constraint" FOREIGN KEY (id_guide_pk) REFERENCES guides(id)
```

Notes:
* Added the id_guide field to the price_variations table
* One constraint for yearly_price_id must be added to the price_variations table.


3. About the scraping algorithm : 
- Variations by make are now being redundantly retrieved from the database since there is no method for calculating real variations. One table could be created for this.
- It is required to update the tables when existing values are being inserted. However, it was not possible due to indexes in the tables which slow down update statements.

4. About the Fasecolda files:
- Errors on csv files caused the change to txt pipe separated files which matched the records in the excel files.
- There is a problem when it comes to obtaining new prices. Some cars are listed in model year older than they actually are. Check for example 13522, the first release of this car was for july 2013 with model 2014, however in both
motor magazine and fasecolda there are 2013 prices.
