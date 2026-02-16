{{ config(materialized='table') }}

with src as (
    select
        name,
        email,
        phone,
        address
    from {{ ref('stg_skill_assessment_raw') }}
),

normalized as (
    select
        -- keep raw values for record_id generation
        coalesce(trim(to_varchar(name)), '')    as name_raw,
        coalesce(trim(to_varchar(email)), '')   as email_raw,
        coalesce(trim(to_varchar(phone)), '')   as phone_raw,
        coalesce(trim(to_varchar(address)), '') as address_raw
    from src
),

-- EMAIL --
email_cleaned as (
    select
        *,
        regexp_replace(lower(email_raw), '\\s+', '') as email_norm,

        case
            when email_raw = '' then ''

            -- must contain @ and at least one dot AFTER the @
            when not regexp_like(
                regexp_replace(lower(email_raw), '\\s+', ''),
                '^[^@]+@[^@]+\\.[^@]+$'
            ) then ''

            else regexp_replace(lower(email_raw), '\\s+', '')
        end as email_clean
    from normalized
),

-- PHONE --
phone_cleaned as (
    select
        *,
        --remove extensions
        regexp_replace(
  phone_raw,
  '\\s*(ext\\.?|x)\\s*\\d+.*$',
  '',
  1,
  0,
  'i'
) as phone_no_ext

    from email_cleaned
),

phone_digits as (
    select
        *,
        regexp_replace(phone_no_ext, '[^0-9]', '') as phone_digits_raw
    from phone_cleaned
),

phone_final as (
    select
        *,
        case
            when phone_digits_raw = '' then ''
            when length(phone_digits_raw) = 11 and left(phone_digits_raw, 1) = '1'
                then substr(phone_digits_raw, 2, 10)
            else phone_digits_raw
        end as phone_digits_10
    from phone_digits
),

phone_formatted as (
    select
        *,
        case
            when length(phone_digits_10) != 10 then ''
            else '(' || substr(phone_digits_10, 1, 3) || ') ' || substr(phone_digits_10, 4, 3) || ' - ' || substr(phone_digits_10, 7, 4)
        end as phone_clean
    from phone_final
),

-- name --
name_normalized as (
    select
        *,
        -- strip wrapping quotes and collapse whitespace
        trim(regexp_replace(regexp_replace(name_raw, '^["'']+|["'']+$', ''), '\\s+', ' ')) as name_trimmed
    from phone_formatted
),

name_reordered as (
    select
        *,
        case
            -- convert "Last, First ..." -> "First ... Last"
            when position(',' in name_trimmed) > 0 then
                trim(
                    regexp_replace(
                        split_part(name_trimmed, ',', 2) || ' ' || split_part(name_trimmed, ',', 1),
                        '\\s+',
                        ' '
                    )
                )
            else name_trimmed
        end as name_ordered
    from name_normalized
),

name_tokens as (
    select
        *,
        split(
            regexp_replace(name_ordered, '\\s+', ' '),
            ' '
        ) as name_arr
    from name_reordered
),


name_split as (
    select
        *,
        array_size(name_arr) as name_len,

        case
            when name_ordered = '' then ''
            when array_size(name_arr) = 1 then ''                            -- single token => last_name only
            else initcap(lower(name_arr[0]))
        end as first_name,

        case
            when name_ordered = '' then ''
            when array_size(name_arr) <= 2 then ''                           -- no middle
            else initcap(lower(array_to_string(array_slice(name_arr, 1, array_size(name_arr) - 2), ' ')))
        end as middle_name,

        case
            when name_ordered = '' then ''
            when array_size(name_arr) = 1 then initcap(lower(name_arr[0]))
            else initcap(lower(name_arr[array_size(name_arr) - 1]))
        end as last_name
    from name_tokens
),

-- address --
address_normalized as (
    select
        *,
        trim(regexp_replace(regexp_replace(address_raw, '^["'']+|["'']+$', ''), '\\s+', ' ')) as address_trimmed
    from name_split
),

address_parts as (
    select
        *,
        split(address_trimmed, ',') as addr_arr
    from address_normalized
),

address_basic as (
    select
        *,
        -- street = first comma part
        case when address_trimmed = '' then '' else initcap(lower(trim(addr_arr[0]::string))) end as street_address,
        -- city = second comma part (if exists)
        case when array_size(addr_arr) >= 2 then initcap(lower(trim(addr_arr[1]::string))) else '' end as city,

        -- tail = everything after city joined
        case
            when array_size(addr_arr) >= 3
                then trim(regexp_replace(array_to_string(array_slice(addr_arr, 2, array_size(addr_arr)), ' '), '\\s+', ' '))
            else ''
        end as tail
    from address_parts
),

address_tail_clean as (
    select
        *,
        -- remove USA / United States text
        trim(regexp_replace(
    tail,
    '\\b(united states|usa)\\b',
    '',
    1,
    0,
    'i'
) 
)as tail_no_country
    from address_basic
),

zip_extracted as (
    select
        *,
        coalesce(regexp_substr(tail_no_country, '[0-9]{5}(-[0-9]{4})?'), '') as zip_raw
    from address_tail_clean
),

zip_cleaned as (
    select
        *,
        case
            when zip_raw = '' then ''
            else lpad(substr(regexp_replace(zip_raw, '[^0-9]', ''), 1, 5), 5, '0')
        end as zip
    from zip_extracted
),

tail_minus_zip as (
    select
        *,
        trim(regexp_replace(tail_no_country, '[0-9]{5}(-[0-9]{4})?', '')) as tail_wo_zip
    from zip_cleaned
),

state_mapped as (
    select
        *,
        -- state candidate: last one or two tokens
        split(trim(regexp_replace(tail_wo_zip, '\\s+', ' ')), ' ') as tail_tokens
    from tail_minus_zip
),

state_final as (
    select
        *,
        -- last token
        upper(coalesce(tail_tokens[array_size(tail_tokens)-1]::string, '')) as state_1,
        -- last two tokens joined
        lower(
            trim(
                coalesce(tail_tokens[array_size(tail_tokens)-2]::string, '') || ' ' ||
                coalesce(tail_tokens[array_size(tail_tokens)-1]::string, '')
            )
        ) as state_2
    from state_mapped
),

state_cleaned as (
    select
        *,
        case
            when address_trimmed = '' then ''
            -- already 2-letter state
            when regexp_like(state_1, '^[A-Z]{2}$') then state_1

            -- map full names (include DC)
            when state_2 = 'new york' then 'NY'
            when state_2 = 'new jersey' then 'NJ'
            when state_2 = 'new mexico' then 'NM'
            when state_2 = 'new hampshire' then 'NH'
            when state_2 = 'north carolina' then 'NC'
            when state_2 = 'north dakota' then 'ND'
            when state_2 = 'south carolina' then 'SC'
            when state_2 = 'south dakota' then 'SD'
            when state_2 = 'west virginia' then 'WV'
            when state_2 = 'rhode island' then 'RI'
            when state_2 = 'district of columbia' then 'DC'

            when lower(state_1) = 'illinois' then 'IL'
            when lower(state_1) = 'california' then 'CA'
            when lower(state_1) = 'texas' then 'TX'
            when lower(state_1) = 'florida' then 'FL'
            when lower(state_1) = 'georgia' then 'GA'
            when lower(state_1) = 'washington' then 'WA'
            when lower(state_1) = 'wisconsin' then 'WI'
            when lower(state_1) = 'indiana' then 'IN'
            when lower(state_1) = 'ohio' then 'OH'
            when lower(state_1) = 'pennsylvania' then 'PA'
            when lower(state_1) = 'virginia' then 'VA'
            when lower(state_1) = 'maryland' then 'MD'
            when lower(state_1) = 'michigan' then 'MI'
            when lower(state_1) = 'minnesota' then 'MN'
            when lower(state_1) = 'arizona' then 'AZ'
            when lower(state_1) = 'colorado' then 'CO'
            when lower(state_1) = 'massachusetts' then 'MA'
            when lower(state_1) = 'tennessee' then 'TN'
            when lower(state_1) = 'missouri' then 'MO'
            when lower(state_1) = 'kentucky' then 'KY'
            when lower(state_1) = 'louisiana' then 'LA'
            when lower(state_1) = 'alabama' then 'AL'
            when lower(state_1) = 'alaska' then 'AK'
            when lower(state_1) = 'arkansas' then 'AR'
            when lower(state_1) = 'connecticut' then 'CT'
            when lower(state_1) = 'delaware' then 'DE'
            when lower(state_1) = 'hawaii' then 'HI'
            when lower(state_1) = 'idaho' then 'ID'
            when lower(state_1) = 'iowa' then 'IA'
            when lower(state_1) = 'kansas' then 'KS'
            when lower(state_1) = 'maine' then 'ME'
            when lower(state_1) = 'mississippi' then 'MS'
            when lower(state_1) = 'montana' then 'MT'
            when lower(state_1) = 'nebraska' then 'NE'
            when lower(state_1) = 'nevada' then 'NV'
            when lower(state_1) = 'oregon' then 'OR'
            when lower(state_1) = 'oklahoma' then 'OK'
            when lower(state_1) = 'utah' then 'UT'
            when lower(state_1) = 'vermont' then 'VT'
            when lower(state_1) = 'wyoming' then 'WY'
            when lower(state_1) = 'north' then ''  -- guard weird token splits
            when lower(state_1) = 'south' then ''
            else ''
        end as state
    from state_final
)

select
    -- deterministic id: hash of raw strings + row_number tie-breaker
    md5(name_raw || '|' || email_raw || '|' || phone_raw || '|' || address_raw || '|' ||
        to_varchar(row_number() over (order by name_raw, email_raw, phone_raw, address_raw))
    ) as record_id,

    current_timestamp() as load_ts,

    first_name,
    middle_name,
    last_name,

    email_clean as email,
    phone_clean as phone,

    street_address,
    city,
    state,
    zip

from state_cleaned
