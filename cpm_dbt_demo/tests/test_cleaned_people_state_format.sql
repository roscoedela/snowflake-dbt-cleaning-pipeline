select *
from {{ ref('cleaned_people') }}
where state <> ''
  and not regexp_like(state, '^[A-Z]{2}$')