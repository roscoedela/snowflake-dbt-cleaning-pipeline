select *
from {{ ref('cleaned_people') }}
where phone <> ''
  and not regexp_like(phone, '^\\([0-9]{3}\\) [0-9]{3} - [0-9]{4}$')