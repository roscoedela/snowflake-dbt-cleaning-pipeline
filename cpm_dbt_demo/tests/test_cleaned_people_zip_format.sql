select *
from {{ ref('cleaned_people') }}
where zip <> ''
  and not regexp_like(zip, '^[0-9]{5}$')