select
name,
email,
phone,
address
from {{ source('raw', 'skill_assessment_raw') }}   