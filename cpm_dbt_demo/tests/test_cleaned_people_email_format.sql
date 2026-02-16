select *
from {{ ref('cleaned_people') }}
where email <> ''
  and not regexp_like(email, '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')
